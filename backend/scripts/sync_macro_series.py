from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from app.config import get_settings
from app.database import Database
from app.services.ingestion.fred import FredIngestionService
from app.services.series_registry import FRED_SERIES_REGISTRY


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync curated FRED macro series into SQLite.")
    parser.add_argument("--db-path", type=Path, default=get_settings().sqlite_path)
    parser.add_argument(
        "--series",
        nargs="*",
        default=[item["series_id"] for item in FRED_SERIES_REGISTRY],
    )
    parser.add_argument("--start-date", type=date.fromisoformat, default=None)
    parser.add_argument("--end-date", type=date.fromisoformat, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    database = Database(args.db_path)
    database.initialize()
    service = FredIngestionService(database, api_key=get_settings().fred_api_key)
    row_count = service.sync_curated_series(
        series_ids=args.series,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(f"Synced {row_count} FRED observations into {args.db_path}")


if __name__ == "__main__":
    main()
