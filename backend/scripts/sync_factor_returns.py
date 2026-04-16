from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from app.config import get_settings
from app.database import Database
from app.services.factor_registry import FACTOR_DATASET_REGISTRY
from app.services.ingestion.factor_returns import FactorReturnsIngestionService


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Sync Kenneth French factor-return datasets into the configured database.")
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=[str(item["dataset_id"]) for item in FACTOR_DATASET_REGISTRY],
    )
    parser.add_argument("--start-date", type=date.fromisoformat, default=None)
    parser.add_argument("--end-date", type=date.fromisoformat, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    database = Database(args.database_url or args.db_path or settings.sqlite_path)
    database.initialize()
    service = FactorReturnsIngestionService(database)
    row_count = service.sync_datasets(
        dataset_ids=args.datasets,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    print(f"Synced {row_count} factor-return observations into {database.display_target}")


if __name__ == "__main__":
    main()
