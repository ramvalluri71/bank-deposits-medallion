"""BRONZE: raw, append-only ingestion of every source feed with Auto Loader.

Files land in /Volumes/<catalog>/<prefix>bronze/landing/<entity>/ as JSON lines. Bronze keeps the
data as delivered (plus lineage columns); unexpected columns go to _rescued_data instead of failing.

Each feed has an explicit schema (its data contract) rather than an inferred one, so the pipeline
also runs when a landing folder is still empty (e.g. prod before the first real feed arrives).
Dates and timestamps stay STRING here, exactly as delivered; silver casts them.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bank.catalog")
BRONZE = f"{CATALOG}.{spark.conf.get('bank.bronze_schema')}"
LANDING = spark.conf.get("bank.landing_path")

SOURCES = {
    "customers": (
        "customer_id STRING, first_name STRING, last_name STRING, date_of_birth STRING, email STRING, "
        "phone STRING, kyc_status STRING, customer_since STRING, updated_at STRING"
    ),
    "customer_address": (
        "customer_id STRING, address_type STRING, address_line1 STRING, address_line2 STRING, city STRING, "
        "state STRING, postal_code STRING, country STRING, updated_at STRING"
    ),
    "customer_accounts": (
        "account_id STRING, customer_id STRING, account_type STRING, currency STRING, status STRING, "
        "opened_date STRING, interest_rate DOUBLE, updated_at STRING"
    ),
    "deposits": "deposit_id STRING, account_id STRING, amount DOUBLE, channel STRING, deposit_ts STRING",
    "withdrawals": "withdrawal_id STRING, account_id STRING, amount DOUBLE, channel STRING, withdrawal_ts STRING",
}


def define_bronze_table(entity: str, schema: str) -> None:
    @dp.table(
        name=f"{BRONZE}.{entity}",
        comment=f"Raw {entity} feed ingested with Auto Loader (append-only).",
        table_properties={"quality": "bronze"},
    )
    def bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("rescuedDataColumn", "_rescued_data")  # columns not in the contract land here
            .schema(schema)
            .load(f"{LANDING}/{entity}/")
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )


for _entity, _schema in SOURCES.items():
    define_bronze_table(_entity, _schema)
