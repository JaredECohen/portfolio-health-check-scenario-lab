from __future__ import annotations

import argparse
from pathlib import Path

from app.config import get_settings
from app.database import Database
from app.services.ingestion.sec_bulk import DEFAULT_METRICS, SecBulkIngestionService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh SEC company facts and submissions for the dim_company universe in batches."
    )
    parser.add_argument("--db-path", type=Path, default=get_settings().sqlite_path)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--start-offset", type=int, default=0)
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Only request companies that do not yet have local fundamentals.",
    )
    parser.add_argument(
        "--metrics",
        nargs="*",
        default=sorted(DEFAULT_METRICS),
        help="Subset of SEC company facts metrics to ingest.",
    )
    parser.add_argument("--user-agent", default=get_settings().sec_user_agent)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    database = Database(args.db_path)
    database.initialize()
    service = SecBulkIngestionService(database, user_agent=args.user_agent)
    metrics = set(args.metrics)
    batch_count = 0
    offset = args.start_offset
    totals = {"companies": 0, "fundamentals": 0, "filings": 0}

    while True:
        if args.max_batches is not None and batch_count >= args.max_batches:
            break
        result = service.bootstrap_sample_from_dim_company(
            metrics=metrics,
            max_companies=args.batch_size,
            offset=offset,
            only_missing=args.only_missing,
            source="sec_refresh",
        )
        batch_count += 1
        totals["companies"] += result.companies
        totals["fundamentals"] += result.fundamentals
        totals["filings"] += result.filings
        print(
            "SEC refresh batch complete:",
            {
                "batch": batch_count,
                "offset": offset,
                "companies": result.companies,
                "fundamentals": result.fundamentals,
                "filings": result.filings,
                "totals": totals.copy(),
            },
            flush=True,
        )
        if result.companies == 0 and result.fundamentals == 0 and result.filings == 0:
            break
        if args.only_missing:
            offset = 0
        else:
            offset += args.batch_size

    print(
        "SEC refresh finished:",
        {
            "batches": batch_count,
            "companies": totals["companies"],
            "fundamentals": totals["fundamentals"],
            "filings": totals["filings"],
        },
    )


if __name__ == "__main__":
    main()
