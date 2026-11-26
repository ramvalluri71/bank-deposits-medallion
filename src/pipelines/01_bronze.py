"""BRONZE: raw, append-only ingestion of every source feed with Auto Loader.

Files land in /Volumes/<catalog>/<prefix>bronze/landing/<entity>/ as JSON lines. Bronze keeps the
data as delivered (plus lineage columns); unexpected columns go to _rescued_data instead of failing.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bank.catalog")
BRONZE = f"{CATALOG}.{spark.conf.get('bank.bronze_schema')}"
LANDING = spark.conf.get("bank.landing_path")

SOURCES = {
    "customers": "customer_id STRING, date_of_birth STRING, customer_since STRING, updated_at STRING",
    "customer_address": "customer_id STRING, postal_code STRING, updated_at STRING",
    "customer_accounts": "account_id STRING, customer_id STRING, interest_rate DOUBLE, updated_at STRING",
    "deposits": "deposit_id STRING, account_id STRING, amount DOUBLE, deposit_ts STRING",
    "withdrawals": "withdrawal_id STRING, account_id STRING, amount DOUBLE, withdrawal_ts STRING",
}


def define_bronze_table(entity: str, schema_hints: str) -> None:
    @dp.table(
        name=f"{BRONZE}.{entity}",
        comment=f"Raw {entity} feed ingested with Auto Loader (append-only).",
        table_properties={"quality": "bronze"},
    )
    def bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", schema_hints)
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .load(f"{LANDING}/{entity}/")
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )


for _entity, _hints in SOURCES.items():
    define_bronze_table(_entity, _hints)
