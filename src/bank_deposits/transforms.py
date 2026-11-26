"""DataFrame transformations shared by the pipeline and the unit tests.

Kept free of any pipeline decorators so they can be tested with a local SparkSession.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

HIGH_TIMESTAMP = "9999-12-31 23:59:59"
TXN_SIGN = {"DEPOSIT": 1, "WITHDRAWAL": -1}


def clean_customers(df: DataFrame) -> DataFrame:
    return df.select(
        F.trim("customer_id").alias("customer_id"),
        F.initcap(F.trim("first_name")).alias("first_name"),
        F.initcap(F.trim("last_name")).alias("last_name"),
        F.lower(F.trim("email")).alias("email"),
        F.regexp_replace("phone", r"[^0-9+]", "").alias("phone"),
        F.to_date("date_of_birth").alias("date_of_birth"),
        F.upper(F.trim("kyc_status")).alias("kyc_status"),
        F.to_date("customer_since").alias("customer_since"),
        F.to_timestamp("updated_at").alias("updated_at"),
    )


def clean_customer_addresses(df: DataFrame) -> DataFrame:
    digits = F.regexp_replace(F.col("postal_code").cast("string"), r"[^0-9]", "")
    return df.select(
        F.trim("customer_id").alias("customer_id"),
        F.upper(F.trim("address_type")).alias("address_type"),
        F.trim("address_line1").alias("address_line1"),
        F.trim("address_line2").alias("address_line2"),
        F.initcap(F.trim("city")).alias("city"),
        F.upper(F.trim("state")).alias("state"),
        F.lpad(F.substring(digits, 1, 5), 5, "0").alias("postal_code"),
        F.upper(F.coalesce(F.trim("country"), F.lit("US"))).alias("country"),
        F.to_timestamp("updated_at").alias("updated_at"),
    )


def clean_customer_accounts(df: DataFrame) -> DataFrame:
    return df.select(
        F.trim("account_id").alias("account_id"),
        F.trim("customer_id").alias("customer_id"),
        F.upper(F.trim("account_type")).alias("account_type"),
        F.upper(F.trim("currency")).alias("currency"),
        F.upper(F.trim("status")).alias("status"),
        F.to_date("opened_date").alias("opened_date"),
        F.col("interest_rate").cast("decimal(6,4)").alias("interest_rate"),
        F.to_timestamp("updated_at").alias("updated_at"),
    )


def normalize_transactions(df: DataFrame, id_col: str, ts_col: str, txn_type: str) -> DataFrame:
    """Map a deposits or withdrawals feed onto the common silver.transactions shape."""
    if txn_type not in TXN_SIGN:
        raise ValueError(f"txn_type must be one of {sorted(TXN_SIGN)}, got {txn_type!r}")
    amount = F.col("amount").cast("decimal(18,2)")
    return df.select(
        F.trim(id_col).alias("transaction_id"),
        F.trim("account_id").alias("account_id"),
        F.lit(txn_type).alias("txn_type"),
        amount.alias("amount"),
        (amount * F.lit(TXN_SIGN[txn_type])).cast("decimal(18,2)").alias("signed_amount"),
        F.upper(F.trim("channel")).alias("channel"),
        F.to_timestamp(ts_col).alias("txn_ts"),
        F.col("_ingest_ts") if "_ingest_ts" in df.columns else F.current_timestamp().alias("_ingest_ts"),
    )


def as_of_join(
    facts: DataFrame,
    scd2: DataFrame,
    keys: list[str],
    ts_col: str,
    start_col: str = "__START_AT",
    end_col: str = "__END_AT",
) -> DataFrame:
    """Left-join each fact row to the SCD2 version that was valid at the fact's timestamp.

    A version is valid on [start, end); an open version has end = NULL.
    """
    f, d = facts.alias("f"), scd2.alias("d")
    cond = [F.col(f"f.{k}") == F.col(f"d.{k}") for k in keys]
    cond += [
        F.col(f"f.{ts_col}") >= F.col(f"d.{start_col}"),
        F.col(f"f.{ts_col}") < F.coalesce(F.col(f"d.{end_col}"), F.lit(HIGH_TIMESTAMP).cast("timestamp")),
    ]
    dim_cols = [F.col(f"d.{c}") for c in scd2.columns if c not in keys]
    return f.join(d, cond, "left").select("f.*", *dim_cols)
