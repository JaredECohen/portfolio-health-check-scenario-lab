from __future__ import annotations

import argparse
from pathlib import Path

from app.config import get_settings
from app.database import Database
from app.services.ingestion.sec_bulk import DEFAULT_METRICS, SecBulkIngestionService


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Bootstrap SEC bulk company facts and submissions into the configured database."
    )
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--max-companies", type=int, default=None)
    parser.add_argument(
        "--sample-from-dim-company",
        action="store_true",
        help="Fetch per-company SEC JSON payloads for the first N CIKs already present in dim_company instead of downloading bulk ZIPs.",
    )
    parser.add_argument(
        "--metrics",
        nargs="*",
        default=sorted(DEFAULT_METRICS),
        help="Subset of SEC company facts metrics to ingest.",
    )
    parser.add_argument("--companyfacts-zip", type=Path, default=None)
    parser.add_argument("--submissions-zip", type=Path, default=None)
    parser.add_argument("--user-agent", default=settings.sec_user_agent)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    database = Database(args.database_url or args.db_path or settings.sqlite_path)
    database.initialize()
    service = SecBulkIngestionService(database, user_agent=args.user_agent)
    metrics = set(args.metrics)
    if args.sample_from_dim_company:
        result = service.bootstrap_sample_from_dim_company(
            metrics=metrics,
            max_companies=args.max_companies or 10,
        )
    elif args.companyfacts_zip and args.submissions_zip:
        result = service.load_from_directory(
            companyfacts_path=args.companyfacts_zip,
            submissions_path=args.submissions_zip,
            metrics=metrics,
            max_companies=args.max_companies,
        )
    else:
        result = service.bootstrap(metrics=metrics, max_companies=args.max_companies)
    print(
        "SEC bootstrap complete:",
        {
            "companies": result.companies,
            "fundamentals": result.fundamentals,
            "filings": result.filings,
        },
    )


if __name__ == "__main__":
    main()
