from __future__ import annotations

import argparse
from pathlib import Path

from app.config import get_settings
from app.database import Database
from app.services.ingestion.sec_bulk import DEFAULT_METRICS, SecBulkIngestionService


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(
        description="Refresh SEC company facts and submissions for the dim_company universe in batches."
    )
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--database-url", default=settings.database_url)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--start-offset", type=int, default=0)
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume from the latest completed sec_refresh batch when available.",
    )
    parser.add_argument(
        "--cleanup-stale",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Mark older running sec_refresh/sec_bulk runs as failed before continuing.",
    )
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
    parser.add_argument("--user-agent", default=settings.sec_user_agent)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = get_settings()
    database = Database(args.database_url or args.db_path or settings.sqlite_path)
    database.initialize()
    service = SecBulkIngestionService(database, user_agent=args.user_agent)
    metrics = set(args.metrics)
    batch_count = 0
    if args.cleanup_stale:
        cleaned = service.cleanup_stale_runs(
            sources=["sec_bulk", "sec_refresh"],
            reason="Superseded by refresh_sec_company_data resume workflow.",
        )
        if cleaned:
            print(
                "SEC refresh cleaned stale runs:",
                {"cleaned_runs": cleaned, "sources": ["sec_bulk", "sec_refresh"]},
                flush=True,
            )
    total_targets = service.dim_company_seed_count(only_missing=args.only_missing)
    offset = args.start_offset
    if args.resume and not args.only_missing:
        offset = service.resume_offset_for_refresh(source="sec_refresh", fallback_offset=args.start_offset)
    totals = {"companies": 0, "fundamentals": 0, "filings": 0}
    print(
        "SEC refresh starting:",
        {
            "resume": args.resume,
            "only_missing": args.only_missing,
            "start_offset": args.start_offset,
            "effective_offset": offset,
            "batch_size": args.batch_size,
            "target_rows": total_targets,
        },
        flush=True,
    )

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
        processed = min(total_targets, offset + result.requested_companies) if total_targets else result.requested_companies
        progress_pct = round((processed / total_targets) * 100, 2) if total_targets else None
        print(
            "SEC refresh batch complete:",
            {
                "batch": batch_count,
                "offset": offset,
                "next_offset": result.next_offset,
                "requested_companies": result.requested_companies,
                "requested_unique_ciks": result.requested_unique_ciks,
                "failed_companies": result.failed_companies,
                "companies": result.companies,
                "fundamentals": result.fundamentals,
                "filings": result.filings,
                "processed_rows": processed,
                "target_rows": total_targets,
                "progress_pct": progress_pct,
                "totals": totals.copy(),
            },
            flush=True,
        )
        if result.requested_companies == 0:
            break
        if args.only_missing:
            offset = 0
        else:
            offset = result.next_offset if result.next_offset is not None else offset + result.requested_companies

    print(
        "SEC refresh finished:",
        {
            "batches": batch_count,
            "final_offset": offset,
            "target_rows": total_targets,
            "companies": totals["companies"],
            "fundamentals": totals["fundamentals"],
            "filings": totals["filings"],
        },
    )


if __name__ == "__main__":
    main()
