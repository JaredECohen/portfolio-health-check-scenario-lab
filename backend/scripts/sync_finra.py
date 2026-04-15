from __future__ import annotations

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Placeholder for FINRA short-interest ingestion. Implement after SEC/FRED milestone."
    )
    return parser.parse_args()


def main() -> None:
    parse_args()
    raise SystemExit(
        "FINRA ingestion is not implemented yet. This milestone focused on SEC bulk bootstrap and curated FRED sync."
    )


if __name__ == "__main__":
    main()
