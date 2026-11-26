import json

from bank_deposits.data_generator import ENTITIES, account_ids, generate_batch, write_batch


def test_batch_is_deterministic():
    assert generate_batch(1, num_customers=10) == generate_batch(1, num_customers=10)


def test_initial_batch_has_every_entity():
    data = generate_batch(1, num_customers=20)
    assert set(data) == set(ENTITIES)
    assert len(data["customers"]) == 20
    assert {a["customer_id"] for a in data["customer_address"] if a["address_type"] == "HOME"} == {
        c["customer_id"] for c in data["customers"]
    }
    assert len(data["customer_accounts"]) == sum(len(account_ids(i)) for i in range(1, 21))


def test_later_batch_moves_customers_after_initial_load():
    first, second = generate_batch(1, num_customers=20), generate_batch(2, num_customers=20)
    assert second["customer_address"], "batch 2 should contain address changes for SCD2"
    initial_ts = max(a["updated_at"] for a in first["customer_address"])
    assert all(a["updated_at"] > initial_ts for a in second["customer_address"])
    assert all(a["address_type"] == "HOME" for a in second["customer_address"])


def test_transactions_reference_known_accounts_except_injected_bad_rows():
    data = generate_batch(1, num_customers=20)
    known = {a["account_id"] for a in data["customer_accounts"]}
    good_deposits = [d for d in data["deposits"] if "BAD" not in d["deposit_id"]]
    assert good_deposits and all(d["account_id"] in known and d["amount"] > 0 for d in good_deposits)
    bad = [w for w in data["withdrawals"] if "BAD" in w["withdrawal_id"]]
    assert len(bad) == 1 and bad[0]["account_id"] is None


def test_write_batch_writes_json_lines(tmp_path):
    data = generate_batch(1, num_customers=5)
    counts = write_batch(str(tmp_path), 1, data)
    lines = (tmp_path / "customers" / "batch_0001.json").read_text().splitlines()
    assert len(lines) == counts["customers"] == 5
    assert json.loads(lines[0])["customer_id"] == "C000001"
