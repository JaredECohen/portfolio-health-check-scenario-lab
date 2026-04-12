from __future__ import annotations

import argparse
import json
from pathlib import Path

import httpx


SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
ETF_PATTERNS = (
    " ETF",
    " TRUST",
    " FUND",
    " ISHARES",
    " SPDR",
    " INVESCO",
    " VANGUARD",
    " PROSHARES",
    " DIREXION",
    " GLOBAL X",
    " INDEX",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build local U.S. public equity ticker metadata.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "tickers" / "us_equities.json",
    )
    parser.add_argument(
        "--user-agent",
        default="Portfolio Health Check research@example.com",
        help="SEC-compliant user agent header.",
    )
    return parser.parse_args()


def should_include(company_name: str) -> bool:
    upper = company_name.upper()
    return not any(pattern in upper for pattern in ETF_PATTERNS)


def normalize_records(payload: dict) -> list[dict]:
    rows = payload.get("data", payload if isinstance(payload, list) else [])
    normalized = []
    for row in rows:
        if isinstance(row, list):
            ticker = row[2]
            company_name = row[1]
            cik = row[0]
            exchange = row[3] if len(row) > 3 else None
        else:
            ticker = row.get("ticker")
            company_name = row.get("name") or row.get("title")
            cik = row.get("cik")
            exchange = row.get("exchange")
        if not ticker or not company_name or not cik:
            continue
        if not should_include(company_name):
            continue
        normalized.append(
            {
                "ticker": str(ticker).upper(),
                "company_name": company_name,
                "cik": str(cik).zfill(10),
                "exchange": exchange,
                "sector": None,
                "asset_type": "Equity",
            }
        )
    normalized.sort(key=lambda item: item["ticker"])
    return normalized


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": args.user_agent, "Accept-Encoding": "gzip, deflate"}
    response = httpx.get(SEC_TICKERS_URL, headers=headers, timeout=30.0)
    response.raise_for_status()
    payload = response.json()
    normalized = normalize_records(payload)
    args.output.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
    print(f"Wrote {len(normalized)} equity tickers to {args.output}")


if __name__ == "__main__":
    main()

