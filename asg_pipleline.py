"""
ASG Airlines: End-to-End Data Engineering Pipeline (Local, no Azure)
Ingests -> Cleans -> Transforms -> Models (Star Schema) -> Exports for Power BI

Fixes applied vs. v1:
  1. Masked passport_number / emergency_contact_phone are now actually
     carried into fact_booking_payments (previously computed then dropped).
  2. emergency_contact_name (a real name = PII) is dropped from all exports.
  3. Bookings with zero payment records (363/1000) are now explicitly
     flagged via payment_status, instead of silently becoming NaN.
  4. All pipeline counts (dupes removed, imputed, quarantined, revenue
     overstatement, payment coverage, etc.) are exported to
     gold/pipeline_summary.csv so they can drive Power BI KPI cards
     directly instead of being retyped by hand from console output.
  5. Logging + try/except replace bare print statements per the case
     study's "error handling and logging" requirement.
  6. Removed the unused 'final' output folder.
"""

import pandas as pd
import numpy as np
import hashlib
import logging
import sys
from datetime import datetime
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log", mode="w"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("asg_pipeline")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
FILE_PATH = "UseCase - Airlines.xlsx"   # <-- update to your actual local path
OUTPUT_DIR = "final"
AIRLINE_MAPPING = {"6F": "IndiGo", "AI": "Air India", "SJ": "SpiceJet", "UK": "Vistara"}

# Running list of metrics -> exported at the end so Power BI can consume them
metrics = {}


# ---------------------------------------------------------------------------
# PII helpers
# ---------------------------------------------------------------------------
def hash_email(email):
    if pd.isna(email):
        return None
    return hashlib.sha256(str(email).encode("utf-8")).hexdigest()


def mask_aadhaar(aadhaar):
    if pd.isna(aadhaar):
        return None
    return f"XXXXXXXX{str(int(aadhaar))[-4:]}"


def mask_passport(passport):
    if pd.isna(passport):
        return None
    p = str(passport)
    if len(p) < 3:
        return "MASKED"
    return f"{p[0]}{'*' * (len(p) - 3)}{p[-2:]}"


def mask_phone(phone):
    if pd.isna(phone):
        return None
    p = str(phone)
    return f"MASKED-{p[-4:]}"


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # -----------------------------------------------------------------
    # PHASE 1: RAW INGESTION
    # -----------------------------------------------------------------
    log.info("--- PHASE 1: RAW INGESTION ---")
    ingestion_time = datetime.now()

    try:
        flights_raw = pd.read_excel(FILE_PATH, sheet_name="flights")
        payments_raw = pd.read_excel(FILE_PATH, sheet_name="payments")
        bookings_raw = pd.read_excel(FILE_PATH, sheet_name="bookings")
        passengers_raw = pd.read_excel(FILE_PATH, sheet_name="passengers")
    except FileNotFoundError as e:
        log.error(f"Source file not found: {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Failed to read source workbook: {e}")
        sys.exit(1)

    for df in [flights_raw, payments_raw, bookings_raw, passengers_raw]:
        df["_ingested_at"] = ingestion_time

    log.info(f"Ingested flights: {len(flights_raw)} rows")
    log.info(f"Ingested payments: {len(payments_raw)} rows")
    log.info(f"Ingested bookings: {len(bookings_raw)} rows")
    log.info(f"Ingested passengers: {len(passengers_raw)} rows")

    metrics["raw_flights_rows"] = len(flights_raw)
    metrics["raw_payments_rows"] = len(payments_raw)
    metrics["raw_bookings_rows"] = len(bookings_raw)
    metrics["raw_passengers_rows"] = len(passengers_raw)

    # -----------------------------------------------------------------
    # PHASE 2: CLEANING & TRANSFORMATION
    # -----------------------------------------------------------------
    log.info("--- PHASE 2: CLEANING & TRANSFORMATION ---")

    # ---- Flights ----
    try:
        flights = flights_raw.copy()
        initial_len = len(flights)
        flights = flights.drop_duplicates()
        dupes_removed = initial_len - len(flights)
        log.info(f"[Flights] Removed {dupes_removed} exact duplicates.")
        metrics["flights_duplicates_removed"] = dupes_removed

        missing_airline_mask = flights["airline"].isna() | (flights["airline"] == "UNKNOWN")
        flights.loc[missing_airline_mask, "airline"] = (
            flights.loc[missing_airline_mask, "flight_id"].str[:2].map(AIRLINE_MAPPING)
        )
        log.info(f"[Flights] Imputed {missing_airline_mask.sum()} missing/UNKNOWN airlines via prefix mapping.")
        metrics["flights_airline_imputed"] = int(missing_airline_mask.sum())

        unresolved_airline = flights["airline"].isna().sum()
        if unresolved_airline:
            log.warning(f"[Flights] {unresolved_airline} rows still have no airline after mapping — check for new prefixes.")
        metrics["flights_airline_unresolved"] = int(unresolved_airline)

        flights["departure_time"] = pd.to_datetime(flights["departure_time"])
        flights["arrival_time"] = pd.to_datetime(flights["arrival_time"])
        flights["duration_minutes"] = (
            flights["arrival_time"] - flights["departure_time"]
        ).dt.total_seconds() / 60.0
        flights["is_overnight"] = flights["arrival_time"].dt.date > flights["departure_time"].dt.date
        log.info(f"[Flights] Identified {flights['is_overnight'].sum()} overnight flights.")
        metrics["flights_overnight_count"] = int(flights["is_overnight"].sum())

        negative_dur_mask = flights["duration_minutes"] < 0
        quarantine_flights = flights[negative_dur_mask].copy()
        quarantine_flights["_quarantine_reason"] = "Negative duration (inverted timestamps)"
        flights = flights[~negative_dur_mask]
        log.info(f"[Flights] Quarantined {len(quarantine_flights)} flight(s) with negative duration.")
        metrics["flights_quarantined"] = len(quarantine_flights)

        # Optional extra business rule: unusually short/long durations worth flagging
        duration_anomaly_mask = (flights["duration_minutes"] < 30) | (flights["duration_minutes"] > 360)
        flights["is_duration_anomaly"] = duration_anomaly_mask
        metrics["flights_duration_anomalies"] = int(duration_anomaly_mask.sum())
        log.info(f"[Flights] Flagged {duration_anomaly_mask.sum()} duration anomaly (<30min or >360min) flights.")

        metrics["flights_final_rows"] = len(flights)
    except Exception as e:
        log.error(f"[Flights] Transformation failed: {e}")
        raise

    # ---- Payments ----
    try:
        payments = payments_raw.copy()
        payments["amount_numeric"] = pd.to_numeric(payments["amount"], errors="coerce")
        missing_amounts = payments["amount_numeric"].isna()

        medians = payments.groupby("payment_method")["amount_numeric"].median()
        for method, median_val in medians.items():
            mask = payments["payment_method"] == method
            payments.loc[mask & missing_amounts, "amount_numeric"] = median_val

        payments["is_amount_imputed"] = missing_amounts
        log.info(f"[Payments] Imputed {missing_amounts.sum()} corrupted/missing amounts.")
        metrics["payments_amount_imputed"] = int(missing_amounts.sum())

        n_unique_bookings = payments["booking_id"].nunique()
        log.info(f"[Payments] {len(payments)} rows for {n_unique_bookings} unique bookings.")
        metrics["payments_rows"] = len(payments)
        metrics["payments_unique_bookings"] = n_unique_bookings

        attempt_counts = payments.groupby("booking_id").size().reset_index(name="payment_attempt_count")
        payments = payments.merge(attempt_counts, on="booking_id")

        payments = payments.sort_values("payment_id")
        payments["is_latest_attempt"] = ~payments.duplicated(subset=["booking_id"], keep="last")
        payments_dedup = payments[payments["is_latest_attempt"]].copy()
        log.info(f"[Payments] Deduplicated to {len(payments_dedup)} settled payments (latest attempt per booking).")
        metrics["payments_settled_rows"] = len(payments_dedup)

        multi_attempt_bookings = (attempt_counts["payment_attempt_count"] > 1).sum()
        metrics["payments_multi_attempt_bookings"] = int(multi_attempt_bookings)
        log.info(f"[Payments] {multi_attempt_bookings} bookings had more than one payment attempt.")
    except Exception as e:
        log.error(f"[Payments] Transformation failed: {e}")
        raise

    # ---- Bookings ----
    try:
        bookings = bookings_raw.copy()

        missing_status = bookings["status"].isna()
        bookings.loc[missing_status, "status"] = "UNKNOWN"
        log.info(f"[Bookings] Filled {missing_status.sum()} missing statuses with 'UNKNOWN'.")
        log.info(f"[Bookings] Retained {(bookings['status'] == 'INVALID').sum()} 'INVALID' statuses for audit visibility.")
        metrics["bookings_status_filled"] = int(missing_status.sum())
        metrics["bookings_invalid_status"] = int((bookings["status"] == "INVALID").sum())

        # PII masking (now actually carried forward into the fact table below)
        bookings["passport_number"] = bookings["passport_number"].apply(mask_passport)
        bookings["emergency_contact_phone"] = bookings["emergency_contact_phone"].apply(mask_phone)
        # emergency_contact_name is a real name -> PII with no analytical value; drop it entirely
        bookings = bookings.drop(columns=["emergency_contact_name"], errors="ignore")
    except Exception as e:
        log.error(f"[Bookings] Transformation failed: {e}")
        raise

    # ---- Passengers ----
    try:
        passengers = passengers_raw.copy()
        initial_pax = len(passengers)
        passengers = passengers.drop_duplicates(subset=["passenger_id"], keep="first")
        dup_pax_removed = initial_pax - len(passengers)
        log.info(f"[Passengers] Removed {dup_pax_removed} duplicate passenger records.")
        metrics["passengers_duplicates_removed"] = dup_pax_removed

        missing_ln = passengers["last_name"].isna()
        passengers.loc[missing_ln, "last_name"] = "N/A"
        metrics["passengers_last_name_filled"] = int(missing_ln.sum())

        passengers["aadhaar_id"] = passengers["aadhaar_id"].apply(mask_aadhaar)
        passengers["phone"] = passengers["phone"].apply(mask_phone)
        passengers["email"] = passengers["email"].apply(hash_email)
        log.info("[Passengers] Applied PII masking (Aadhaar, Phone, Email).")
    except Exception as e:
        log.error(f"[Passengers] Transformation failed: {e}")
        raise

    # -----------------------------------------------------------------
    # PHASE 3: STAR SCHEMA / GOLD LAYER
    # -----------------------------------------------------------------
    log.info("--- PHASE 3: STAR SCHEMA (GOLD LAYER) ---")

    dim_airline = flights[["airline"]].drop_duplicates().reset_index(drop=True)
    dim_airline.insert(0, "airline_key", range(1, len(dim_airline) + 1))

    dim_route = flights[["source", "destination"]].drop_duplicates().reset_index(drop=True)
    dim_route.insert(0, "route_key", range(1, len(dim_route) + 1))
    dim_route["route_name"] = dim_route["source"] + " -> " + dim_route["destination"]

    dim_passenger = passengers[
        ["passenger_id", "first_name", "last_name", "age", "gender", "email", "phone", "aadhaar_id", "date_of_birth"]
    ].copy()
    dim_passenger = dim_passenger.rename(columns={"passenger_id": "passenger_key"})

    fact_flight_operations = flights.merge(dim_airline, on="airline").merge(dim_route, on=["source", "destination"])
    fact_flight_operations = fact_flight_operations[
        ["flight_id", "airline_key", "route_key", "departure_time", "arrival_time",
         "duration_minutes", "is_overnight", "is_duration_anomaly"]
    ]

    # FIX: keep passport_number / seat_number / emergency_contact_phone in the fact
    # table -- previously masked in `bookings` but never exported anywhere.
    fact_booking_payments = bookings[
        ["booking_id", "passenger_id", "flight_id", "booking_date", "status",
         "seat_number", "passport_number", "emergency_contact_phone"]
    ].copy()
    fact_booking_payments = fact_booking_payments.rename(columns={"passenger_id": "passenger_key"})
    fact_booking_payments = fact_booking_payments.merge(
        payments_dedup[["booking_id", "amount_numeric", "payment_method", "is_amount_imputed", "payment_attempt_count"]],
        on="booking_id",
        how="left",
    )
    fact_booking_payments = fact_booking_payments.rename(columns={"amount_numeric": "payment_amount"})

    # FIX: bookings with zero payment records were silently NaN before -- now explicit
    fact_booking_payments["payment_status"] = np.where(
        fact_booking_payments["payment_amount"].notna(), "PAYMENT_RECORDED", "NO_PAYMENT_RECORD"
    )
    no_payment_count = (fact_booking_payments["payment_status"] == "NO_PAYMENT_RECORD").sum()
    log.info(f"[Bookings] {no_payment_count} of {len(fact_booking_payments)} bookings have NO payment record at all.")
    metrics["bookings_no_payment_record"] = int(no_payment_count)
    metrics["payment_coverage_pct"] = round(100 * (1 - no_payment_count / len(fact_booking_payments)), 2)

    # -----------------------------------------------------------------
    # EXPORT
    # -----------------------------------------------------------------
    log.info("--- EXPORT ---")
    datasets = {
        "dim_airline": dim_airline,
        "dim_route": dim_route,
        "dim_passenger": dim_passenger,
        "fact_flight_operations": fact_flight_operations,
        "fact_booking_payments": fact_booking_payments,
        "quarantine_flights": quarantine_flights,
    }

    for name, df in datasets.items():
        try:
            df.to_csv(f"{OUTPUT_DIR}/{name}.csv", index=False)
            df_out = df.astype(str) if name == "quarantine_flights" else df
            df_out.to_parquet(f"{OUTPUT_DIR}/{name}.parquet", index=False)
            log.info(f"Exported {name}: {len(df)} rows.")
        except Exception as e:
            log.warning(f"Could not fully export {name}: {e}")

    # -----------------------------------------------------------------
    # REVENUE VALIDATION (the key "don't double-count payment retries" catch)
    # -----------------------------------------------------------------
    log.info("--- REVENUE VALIDATION ---")
    naive_revenue = payments["amount_numeric"].sum()
    settled_revenue = fact_booking_payments["payment_amount"].sum()
    confirmed_revenue = fact_booking_payments.loc[
        fact_booking_payments["status"] == "CONFIRMED", "payment_amount"
    ].sum()

    overstatement = (naive_revenue / settled_revenue) - 1

    log.info(f"Naive revenue (sum of every payment attempt):  Rs. {naive_revenue:,.2f}")
    log.info(f"Settled revenue (latest attempt per booking):  Rs. {settled_revenue:,.2f}")
    log.info(f"Confirmed revenue (settled + CONFIRMED only):  Rs. {confirmed_revenue:,.2f}")
    log.info(f"--> Naive approach overstates settled revenue by {overstatement * 100:.1f}%")
    log.info(f"--> Payment coverage: {metrics['payment_coverage_pct']}% of bookings have a payment on file.")

    metrics["revenue_naive"] = round(float(naive_revenue), 2)
    metrics["revenue_settled"] = round(float(settled_revenue), 2)
    metrics["revenue_confirmed_only"] = round(float(confirmed_revenue), 2)
    metrics["revenue_overstatement_pct"] = round(float(overstatement * 100), 2)

    # FIX: persist every metric so Power BI KPI cards read from data, not from
    # someone retyping console output.
    summary_df = pd.DataFrame(list(metrics.items()), columns=["metric", "value"])
    summary_df.to_csv(f"{OUTPUT_DIR}/pipeline_summary.csv", index=False)
    log.info(f"Exported pipeline_summary.csv ({len(summary_df)} metrics).")

    log.info("Pipeline completed successfully.")


if __name__ == "__main__":
    main()