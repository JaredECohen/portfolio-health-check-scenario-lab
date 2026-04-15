from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from app.config import get_settings
from app.database import Database


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync local company reference metadata into dim_company.")
    parser.add_argument("--db-path", type=Path, default=get_settings().sqlite_path)
    parser.add_argument("--input-path", type=Path, default=get_settings().ticker_metadata_path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    database = Database(args.db_path)
    database.initialize()
    payload = json.loads(args.input_path.read_text(encoding="utf-8"))
    updated_at = datetime.now(UTC).isoformat()
    inserted = 0
    with database.connect() as connection:
        for item in payload:
            connection.execute(
                """
                INSERT INTO dim_company(ticker, cik, company_name, sector, industry, exchange, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                  cik = excluded.cik,
                  company_name = excluded.company_name,
                  sector = COALESCE(excluded.sector, dim_company.sector),
                  exchange = COALESCE(excluded.exchange, dim_company.exchange),
                  updated_at = excluded.updated_at
                """,
                (
                    item["ticker"],
                    item["cik"],
                    item["company_name"],
                    item.get("sector"),
                    item.get("industry"),
                    item.get("exchange"),
                    updated_at,
                ),
            )
            inserted += 1
    print(f"Synced {inserted} company reference rows into {args.db_path}")


if __name__ == "__main__":
    main()
