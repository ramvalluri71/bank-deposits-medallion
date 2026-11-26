"""Job task: create the bronze/silver/gold schemas and the landing volume (idempotent)."""

import argparse

from pyspark.sql import SparkSession

LAYERS = {
    "bronze": "Raw source feeds as delivered (Auto Loader).",
    "silver": "Cleansed and conformed entities; customer_address is SCD Type 2.",
    "gold": "Business-ready aggregates and dimensional views.",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema-prefix", default="")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    for layer, comment in LAYERS.items():
        schema = f"`{args.catalog}`.`{args.schema_prefix}{layer}`"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema} COMMENT '{comment}'")
        print(f"ensured schema {schema}")
    volume = f"`{args.catalog}`.`{args.schema_prefix}bronze`.`landing`"
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume} COMMENT 'Landing zone for source files'")
    print(f"ensured volume {volume}")


if __name__ == "__main__":
    main()
