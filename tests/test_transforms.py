from datetime import datetime
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")  # skip locally if PySpark isn't installed; CI installs it

from bank_deposits import transforms as T  # noqa: E402


def test_clean_customers_standardises_fields(spark):
    df = spark.createDataFrame(
        [(" C1 ", "ava", "SMITH", " Ava.Smith@Example.COM ", "+1 (555) 123-4567", "1990-02-03",
          "verified", "2026-01-01", "2026-01-01T06:00:00")],
        "customer_id string, first_name string, last_name string, email string, phone string, "
        "date_of_birth string, kyc_status string, customer_since string, updated_at string",
    )
    row = T.clean_customers(df).first()
    assert row.customer_id == "C1"
    assert (row.first_name, row.last_name) == ("Ava", "Smith")
    assert row.email == "ava.smith@example.com"
    assert row.phone == "+15551234567"
    assert row.kyc_status == "VERIFIED"
    assert row.updated_at == datetime(2026, 1, 1, 6, 0)


def test_clean_addresses_pads_postal_code_and_defaults_country(spark):
    df = spark.createDataFrame(
        [("C1", "home", "1 Main St", None, "boston", "ma", "2108", None, "2026-01-01T06:00:00")],
        "customer_id string, address_type string, address_line1 string, address_line2 string, city string, "
        "state string, postal_code string, country string, updated_at string",
    )
    row = T.clean_customer_addresses(df).first()
    assert (row.address_type, row.city, row.state) == ("HOME", "Boston", "MA")
    assert row.postal_code == "02108"
    assert row.country == "US"


@pytest.mark.parametrize("txn_type,expected_sign", [("DEPOSIT", 1), ("WITHDRAWAL", -1)])
def test_normalize_transactions_signs_amount(spark, txn_type, expected_sign):
    df = spark.createDataFrame(
        [("X1", "A1", 100.25, "atm", "2026-01-01T10:00:00")],
        "src_id string, account_id string, amount double, channel string, src_ts string",
    )
    row = T.normalize_transactions(df, "src_id", "src_ts", txn_type).first()
    assert row.transaction_id == "X1"
    assert row.txn_type == txn_type
    assert row.amount == Decimal("100.25")
    assert row.signed_amount == Decimal("100.25") * expected_sign
    assert row.channel == "ATM"


def test_normalize_transactions_rejects_unknown_type(spark):
    df = spark.createDataFrame([("X1",)], "id string")
    with pytest.raises(ValueError):
        T.normalize_transactions(df, "id", "id", "TRANSFER")


def test_as_of_join_picks_address_valid_at_transaction_time(spark):
    history = spark.createDataFrame(
        [
            ("C1", "Austin", datetime(2026, 1, 1), datetime(2026, 1, 5)),
            ("C1", "Denver", datetime(2026, 1, 5), None),
        ],
        "customer_id string, city string, __START_AT timestamp, __END_AT timestamp",
    )
    txns = spark.createDataFrame(
        [
            ("T1", "C1", datetime(2026, 1, 3)),   # before the move
            ("T2", "C1", datetime(2026, 1, 5)),   # exactly at the move -> new version
            ("T3", "C1", datetime(2026, 2, 1)),   # open-ended current version
            ("T4", "C2", datetime(2026, 1, 3)),   # no address history -> null
        ],
        "transaction_id string, customer_id string, txn_ts timestamp",
    )
    result = {r.transaction_id: r.city for r in T.as_of_join(txns, history, ["customer_id"], "txn_ts").collect()}
    assert result == {"T1": "Austin", "T2": "Denver", "T3": "Denver", "T4": None}
