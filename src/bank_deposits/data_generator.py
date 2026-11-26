"""Deterministic synthetic source feeds for the bank deposits lakehouse.

Batch 1 is the initial load (customers, addresses, accounts, transactions). Each later batch
simulates one more business day: some customers move (new HOME address -> SCD2 version),
some update contact details (SCD1), savings rates change, and new deposits/withdrawals arrive.
A couple of intentionally bad records are injected so pipeline expectations have work to do.
Pure Python (no Spark) so it runs anywhere and writes straight to a Unity Catalog volume.
"""

from __future__ import annotations

import json
import os
import random
from datetime import date, datetime, timedelta

ENTITIES = ("customers", "customer_address", "customer_accounts", "deposits", "withdrawals")
BASE_TS = datetime(2026, 1, 1, 6, 0, 0)

FIRST_NAMES = ["Ava", "Liam", "Mia", "Noah", "Zoe", "Ethan", "Priya", "Arjun", "Sofia", "Mateo",
               "Chloe", "Lucas", "Aisha", "Omar", "Grace", "Ravi", "Hana", "Leo", "Nina", "Sam"]
LAST_NAMES = ["Smith", "Patel", "Garcia", "Nguyen", "Johnson", "Kim", "Brown", "Lopez", "Chen",
              "Williams", "Reddy", "Davis", "Martinez", "Khan", "Miller", "Singh", "Wilson", "Lee"]
STREETS = ["Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Elm St", "Pine Rd", "Lake View Blvd",
           "Hillcrest Way", "River Rd", "Sunset Ave"]
CITIES = [("Austin", "TX", "78701"), ("Dallas", "TX", "75201"), ("Denver", "CO", "80202"),
          ("Seattle", "WA", "98101"), ("Chicago", "IL", "60601"), ("Atlanta", "GA", "30303"),
          ("Phoenix", "AZ", "85004"), ("Boston", "MA", "02108"), ("Miami", "FL", "33130"),
          ("San Jose", "CA", "95113"), ("Charlotte", "NC", "28202"), ("Columbus", "OH", "43215")]
ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "MONEY_MARKET"]
BASE_RATES = {"CHECKING": 0.0005, "SAVINGS": 0.0410, "MONEY_MARKET": 0.0450}
DEPOSIT_CHANNELS = ["BRANCH", "ATM", "ACH", "MOBILE", "WIRE"]
WITHDRAWAL_CHANNELS = ["BRANCH", "ATM", "ACH", "DEBIT_CARD", "WIRE"]


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def customer_id(i: int) -> str:
    return f"C{i:06d}"


def account_ids(i: int) -> list[tuple[str, str]]:
    """Every customer has 1-3 accounts; the mapping is stable across batches."""
    n = 1 + (i % 3)
    return [(f"A{i:06d}{k + 1}", ACCOUNT_TYPES[k]) for k in range(n)]


def _profile(i: int, seed: int) -> dict:
    rng = random.Random(f"{seed}-profile-{i}")
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    dob = date(1950, 1, 1) + timedelta(days=rng.randint(0, 365 * 55))
    return {"first_name": first, "last_name": last, "date_of_birth": dob.isoformat(),
            "email": f"{first}.{last}{i}@example.com", "phone": f"+1-555-{rng.randint(1000000, 9999999)}"}


def _address(i: int, address_type: str, rng: random.Random, ts: datetime) -> dict:
    city, state, postal = rng.choice(CITIES)
    return {
        "customer_id": customer_id(i),
        "address_type": address_type,
        "address_line1": f"{rng.randint(1, 9999)} {rng.choice(STREETS)}",
        "address_line2": rng.choice([None, None, f"Apt {rng.randint(1, 999)}"]),
        "city": city,
        "state": state,
        "postal_code": postal,
        "country": "US",
        "updated_at": _ts(ts),
    }


def generate_batch(batch: int, num_customers: int = 100, max_txns_per_account: int = 4,
                   seed: int = 42, inject_bad_records: bool = True) -> dict[str, list[dict]]:
    if batch < 1:
        raise ValueError("batch must be >= 1")
    rng = random.Random(f"{seed}-batch-{batch}")
    day_start = BASE_TS + timedelta(days=batch - 1)
    out: dict[str, list[dict]] = {e: [] for e in ENTITIES}
    customer_range = range(1, num_customers + 1)

    if batch == 1:
        for i in customer_range:
            p = _profile(i, seed)
            out["customers"].append({"customer_id": customer_id(i), **p, "kyc_status": "verified",
                                     "customer_since": day_start.date().isoformat(),
                                     "updated_at": _ts(day_start)})
            out["customer_address"].append(_address(i, "HOME", rng, day_start))
            if rng.random() < 0.3:
                out["customer_address"].append(_address(i, "MAILING", rng, day_start))
            for acct_id, acct_type in account_ids(i):
                out["customer_accounts"].append({
                    "account_id": acct_id, "customer_id": customer_id(i), "account_type": acct_type,
                    "currency": "USD", "status": "ACTIVE", "opened_date": day_start.date().isoformat(),
                    "interest_rate": BASE_RATES[acct_type], "updated_at": _ts(day_start)})
    else:
        change_ts = day_start + timedelta(minutes=rng.randint(0, 59))
        # ~15% of customers move: a new HOME address version (SCD2).
        for i in rng.sample(customer_range, k=max(1, num_customers * 15 // 100)):
            out["customer_address"].append(_address(i, "HOME", rng, change_ts))
        # ~5% of customers update contact details (SCD1 overwrite).
        for i in rng.sample(customer_range, k=max(1, num_customers * 5 // 100)):
            p = _profile(i, seed)
            p["phone"] = f"+1-555-{rng.randint(1000000, 9999999)}"
            p["email"] = f"{p['first_name']}.{p['last_name']}{i}.b{batch}@example.com"
            out["customers"].append({"customer_id": customer_id(i), **p, "kyc_status": "verified",
                                     "customer_since": BASE_TS.date().isoformat(),
                                     "updated_at": _ts(change_ts)})
        # Savings rate change for every savings account (SCD1 overwrite).
        delta = rng.choice([-0.0025, 0.0, 0.0025])
        for i in customer_range:
            for acct_id, acct_type in account_ids(i):
                if acct_type == "SAVINGS" and delta:
                    out["customer_accounts"].append({
                        "account_id": acct_id, "customer_id": customer_id(i), "account_type": acct_type,
                        "currency": "USD", "status": "ACTIVE", "opened_date": BASE_TS.date().isoformat(),
                        "interest_rate": round(BASE_RATES[acct_type] + delta * (batch - 1), 4),
                        "updated_at": _ts(change_ts)})

    # Transactions happen after the day's master-data changes (from 02:00 into the day).
    dep_n = wdl_n = 0
    for i in customer_range:
        for acct_id, _ in account_ids(i):
            for _ in range(rng.randint(0, max_txns_per_account)):
                ts = day_start + timedelta(hours=2, seconds=rng.randint(0, 16 * 3600))
                if rng.random() < 0.6:
                    dep_n += 1
                    out["deposits"].append({"deposit_id": f"DEP-{batch:04d}-{dep_n:07d}", "account_id": acct_id,
                                            "amount": round(rng.uniform(20, 5000), 2),
                                            "channel": rng.choice(DEPOSIT_CHANNELS), "deposit_ts": _ts(ts)})
                else:
                    wdl_n += 1
                    out["withdrawals"].append({"withdrawal_id": f"WDL-{batch:04d}-{wdl_n:07d}",
                                               "account_id": acct_id,
                                               "amount": round(rng.uniform(10, 1500), 2),
                                               "channel": rng.choice(WITHDRAWAL_CHANNELS),
                                               "withdrawal_ts": _ts(ts)})

    if inject_bad_records:
        bad_ts = _ts(day_start + timedelta(hours=3))
        out["deposits"].append({"deposit_id": f"DEP-{batch:04d}-BAD0001", "account_id": "A0000011",
                                "amount": -50.0, "channel": "ATM", "deposit_ts": bad_ts})
        out["withdrawals"].append({"withdrawal_id": f"WDL-{batch:04d}-BAD0001", "account_id": None,
                                   "amount": 25.0, "channel": "ATM", "withdrawal_ts": bad_ts})
    return out


def write_batch(landing_path: str, batch: int, records: dict[str, list[dict]]) -> dict[str, int]:
    """Write one JSON-lines file per entity: <landing>/<entity>/batch_<n>.json."""
    counts = {}
    for entity, rows in records.items():
        counts[entity] = len(rows)
        if not rows:
            continue
        folder = os.path.join(landing_path, entity)
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, f"batch_{batch:04d}.json"), "w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row) + "\n")
    return counts
