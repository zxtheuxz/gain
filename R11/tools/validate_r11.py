"""R11 validation: mesmas funcoes do R10, gates calibrados pra daytrade."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from R10.tools.validate_r10 import validate  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--min-wilson-ci-lower", type=float, default=55.0)
    parser.add_argument("--max-binom-p-mining", type=float, default=0.01)
    parser.add_argument("--max-binom-p-val", type=float, default=0.05)
    parser.add_argument("--wf-min-trades-per-window", type=int, default=3)
    parser.add_argument("--wf-min-win-per-window", type=float, default=60.0)
    parser.add_argument("--min-val-trades", type=int, default=10)
    parser.add_argument("--min-val-win", type=float, default=60.0)
    parser.add_argument("--min-val-avg", type=float, default=0.5)
    parser.add_argument("--max-fdr-q", type=float, default=0.05)
    parser.add_argument("--min-bootstrap-lower", type=float, default=0.0)
    parser.add_argument("--bootstrap-iters", type=int, default=1000)
    args = parser.parse_args()
    return validate(args)


if __name__ == "__main__":
    raise SystemExit(main())
