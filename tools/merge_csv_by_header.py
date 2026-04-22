from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge CSV files that share the same header.")
    parser.add_argument("--output", required=True, help="Output CSV path.")
    parser.add_argument("inputs", nargs="+", help="Input CSV paths.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = Path(args.output)
    input_paths = [Path(item) for item in args.inputs]

    fieldnames: list[str] | None = None
    rows_written = 0
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as output_obj:
        writer: csv.DictWriter[str] | None = None
        for input_path in input_paths:
            if not input_path.exists() or input_path.stat().st_size == 0:
                continue
            with input_path.open(encoding="utf-8", newline="") as input_obj:
                reader = csv.DictReader(input_obj)
                if not reader.fieldnames:
                    continue
                if fieldnames is None:
                    fieldnames = list(reader.fieldnames)
                    writer = csv.DictWriter(output_obj, fieldnames=fieldnames)
                    writer.writeheader()
                elif list(reader.fieldnames) != fieldnames:
                    raise ValueError(f"Header mismatch in {input_path}")
                assert writer is not None
                for row in reader:
                    writer.writerow(row)
                    rows_written += 1

    if fieldnames is None:
        raise SystemExit("No input rows found.")
    print(f"merged_rows={rows_written} -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
