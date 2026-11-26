"""SILVER: cleansed, conformed and de-duplicated entities.

- customers           SCD Type 1 (latest values win)
- customer_address    SCD Type 2 (full history; __START_AT / __END_AT, open row has __END_AT NULL)
- customer_accounts   SCD Type 1
- transactions        deposits + withdrawals unified, signed amounts, de-duplicated
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from bank_deposits import transforms as T

CATALOG = spark.conf.get("bank.catalog")
BRONZE = f"{CATALOG}.{spark.conf.get('bank.bronze_schema')}"
SILVER = f"{CATALOG}.{spark.conf.get('bank.silver_schema')}"

# --------------------------------------------------------------------------- customers (SCD1)


@dp.temporary_view(name="customers_clean")
@dp.expect_all_or_drop({"customer_id_present": "customer_id IS NOT NULL",
                        "updated_at_present": "updated_at IS NOT NULL"})
@dp.expect("email_well_formed", "email LIKE '%_@_%._%'")
def customers_clean():
    return T.clean_customers(spark.readStream.table(f"{BRONZE}.customers"))


dp.create_streaming_table(
    name=f"{SILVER}.customers",
    comment="Current customer profile (SCD Type 1).",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    target=f"{SILVER}.customers",
    source="customers_clean",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=1,
)

# --------------------------------------------------------------------- customer_address (SCD2)


@dp.temporary_view(name="customer_address_clean")
@dp.expect_all_or_drop({"customer_id_present": "customer_id IS NOT NULL",
                        "address_type_valid": "address_type IN ('HOME', 'MAILING')",
                        "updated_at_present": "updated_at IS NOT NULL"})
@dp.expect("postal_code_5_digits", "postal_code RLIKE '^[0-9]{5}$'")
def customer_address_clean():
    return T.clean_customer_addresses(spark.readStream.table(f"{BRONZE}.customer_address"))


dp.create_streaming_table(
    name=f"{SILVER}.customer_address",
    comment="Customer address history (SCD Type 2). Current row: __END_AT IS NULL.",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    target=f"{SILVER}.customer_address",
    source="customer_address_clean",
    keys=["customer_id", "address_type"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=2,
    # Only a real change to the address opens a new version; a re-sent identical row does not.
    track_history_column_list=["address_line1", "address_line2", "city", "state", "postal_code", "country"],
)

# ------------------------------------------------------------------- customer_accounts (SCD1)


@dp.temporary_view(name="customer_accounts_clean")
@dp.expect_all_or_drop({"account_id_present": "account_id IS NOT NULL",
                        "customer_id_present": "customer_id IS NOT NULL",
                        "updated_at_present": "updated_at IS NOT NULL"})
@dp.expect("known_account_type", "account_type IN ('CHECKING', 'SAVINGS', 'MONEY_MARKET', 'CD')")
def customer_accounts_clean():
    return T.clean_customer_accounts(spark.readStream.table(f"{BRONZE}.customer_accounts"))


dp.create_streaming_table(
    name=f"{SILVER}.customer_accounts",
    comment="Current deposit accounts per customer (SCD Type 1).",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    target=f"{SILVER}.customer_accounts",
    source="customer_accounts_clean",
    keys=["account_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=1,
)

# ---------------------------------------------------------------- transactions (append-only)

dp.create_streaming_table(
    name=f"{SILVER}.transactions",
    comment="Deposits and withdrawals in one conformed ledger; withdrawals carry negative signed_amount.",
    table_properties={"quality": "silver"},
    expect_all_or_drop={
        "transaction_id_present": "transaction_id IS NOT NULL",
        "account_id_present": "account_id IS NOT NULL",
        "amount_positive": "amount > 0",
        "txn_ts_present": "txn_ts IS NOT NULL",
    },
)


def _dedupe(df):
    return df.withWatermark("txn_ts", "3 days").dropDuplicatesWithinWatermark(["transaction_id"])


@dp.append_flow(target=f"{SILVER}.transactions", name="deposits_to_transactions")
def deposits_to_transactions():
    raw = spark.readStream.table(f"{BRONZE}.deposits")
    return _dedupe(T.normalize_transactions(raw, "deposit_id", "deposit_ts", "DEPOSIT"))


@dp.append_flow(target=f"{SILVER}.transactions", name="withdrawals_to_transactions")
def withdrawals_to_transactions():
    raw = spark.readStream.table(f"{BRONZE}.withdrawals")
    return _dedupe(T.normalize_transactions(raw, "withdrawal_id", "withdrawal_ts", "WITHDRAWAL"))
