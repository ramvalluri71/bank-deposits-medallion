"""Job task: land one batch of synthetic source files in the landing volume."""

import argparse
import os
import sys

# Make the sibling `bank_deposits` package importable when run as a Databricks job task.
try:
    _here = os.path.dirname(os.path.abspath(__file__))
except NameError:  # some runners do not define __file__
    _here = os.getcwd()
sys.path.insert(0, os.path.abspath(os.path.join(_here, "..")))

from bank_deposits.data_generator import generate_batch, write_batch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing-path", required=True)
    parser.add_argument("--batch", type=int, default=1)
    parser.add_argument("--num-customers", type=int, default=100)
    args = parser.parse_args()

    counts = write_batch(args.landing_path, args.batch, generate_batch(args.batch, args.num_customers))
    for entity, n in counts.items():
        print(f"batch {args.batch}: wrote {n:>5} {entity} records")


if __name__ == "__main__":
    main()
