"""GOLD: business-ready materialized views for reporting and analytics."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from bank_deposits import transforms as T

CATALOG = spark.conf.get("bank.catalog")
SILVER = f"{CATALOG}.{spark.conf.get('bank.silver_schema')}"
GOLD = f"{CATALOG}.{spark.conf.get('bank.gold_schema')}"
GOLD_PROPS = {"quality": "gold"}


@dp.materialized_view(
    name=f"{GOLD}.dim_customer",
    comment="One row per customer with current HOME address and account counts.",
    table_properties=GOLD_PROPS,
)
def dim_customer():
    home = (
        spark.read.table(f"{SILVER}.customer_address")
        .where("__END_AT IS NULL AND address_type = 'HOME'")
        .select(
            "customer_id",
            F.col("address_line1").alias("home_address_line1"),
            F.col("address_line2").alias("home_address_line2"),
            F.col("city").alias("home_city"),
            F.col("state").alias("home_state"),
            F.col("postal_code").alias("home_postal_code"),
            F.col("__START_AT").alias("home_address_since"),
        )
    )
    accounts = (
        spark.read.table(f"{SILVER}.customer_accounts")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("num_accounts"),
            F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("num_active_accounts"),
        )
    )
    return (
        spark.read.table(f"{SILVER}.customers")
        .join(home, "customer_id", "left")
        .join(accounts, "customer_id", "left")
    )


@dp.materialized_view(
    name=f"{GOLD}.fact_transactions",
    comment="Transactions enriched with account and the customer's HOME address as of the transaction time "
    "(point-in-time join on the SCD2 address history).",
    table_properties=GOLD_PROPS,
)
def fact_transactions():
    txns = spark.read.table(f"{SILVER}.transactions").drop("_ingest_ts")
    accounts = spark.read.table(f"{SILVER}.customer_accounts").select("account_id", "customer_id", "account_type")
    home_history = (
        spark.read.table(f"{SILVER}.customer_address")
        .where("address_type = 'HOME'")
        .select(
            "customer_id",
            F.col("city").alias("txn_home_city"),
            F.col("state").alias("txn_home_state"),
            "__START_AT",
            "__END_AT",
        )
    )
    enriched = txns.join(accounts, "account_id", "left")
    return (
        T.as_of_join(enriched, home_history, keys=["customer_id"], ts_col="txn_ts")
        .drop("__START_AT", "__END_AT")
        .withColumn("txn_date", F.to_date("txn_ts"))
    )


@dp.materialized_view(
    name=f"{GOLD}.account_balances",
    comment="Running balance and lifetime deposit/withdrawal totals per account.",
    table_properties=GOLD_PROPS,
)
def account_balances():
    txns = spark.read.table(f"{SILVER}.transactions")
    totals = txns.groupBy("account_id").agg(
        F.sum(F.when(F.col("txn_type") == "DEPOSIT", F.col("amount"))).alias("total_deposits"),
        F.sum(F.when(F.col("txn_type") == "WITHDRAWAL", F.col("amount"))).alias("total_withdrawals"),
        F.sum("signed_amount").alias("current_balance"),
        F.count("*").alias("txn_count"),
        F.max("txn_ts").alias("last_txn_ts"),
    )
    zero = F.lit(0).cast("decimal(18,2)")
    return (
        spark.read.table(f"{SILVER}.customer_accounts")
        .select("account_id", "customer_id", "account_type", "status", "interest_rate")
        .join(totals, "account_id", "left")
        .select(
            "account_id", "customer_id", "account_type", "status", "interest_rate",
            F.coalesce("total_deposits", zero).alias("total_deposits"),
            F.coalesce("total_withdrawals", zero).alias("total_withdrawals"),
            F.coalesce("current_balance", zero).alias("current_balance"),
            F.coalesce("txn_count", F.lit(0)).alias("txn_count"),
            "last_txn_ts",
        )
    )


@dp.materialized_view(
    name=f"{GOLD}.daily_deposit_flows",
    comment="Daily deposit inflow, withdrawal outflow and net flow by account type and customer home state.",
    table_properties=GOLD_PROPS,
)
def daily_deposit_flows():
    return (
        spark.read.table(f"{GOLD}.fact_transactions")
        .groupBy("txn_date", "account_type", "txn_home_state")
        .agg(
            F.sum(F.when(F.col("txn_type") == "DEPOSIT", F.col("amount")).otherwise(0)).alias("deposits"),
            F.sum(F.when(F.col("txn_type") == "WITHDRAWAL", F.col("amount")).otherwise(0)).alias("withdrawals"),
            F.sum("signed_amount").alias("net_flow"),
            F.count("*").alias("txn_count"),
        )
    )
