from __future__ import annotations


FRED_SERIES_REGISTRY: list[dict[str, str]] = [
    {
        "series_id": "DGS10",
        "source": "FRED",
        "category": "rates",
        "frequency": "daily",
        "unit": "percent",
        "title": "10-Year Treasury Constant Maturity Rate",
    },
    {
        "series_id": "DGS2",
        "source": "FRED",
        "category": "rates",
        "frequency": "daily",
        "unit": "percent",
        "title": "2-Year Treasury Constant Maturity Rate",
    },
    {
        "series_id": "FEDFUNDS",
        "source": "FRED",
        "category": "rates",
        "frequency": "monthly",
        "unit": "percent",
        "title": "Effective Federal Funds Rate",
    },
    {
        "series_id": "CPIAUCSL",
        "source": "FRED",
        "category": "inflation",
        "frequency": "monthly",
        "unit": "index",
        "title": "Consumer Price Index for All Urban Consumers",
    },
    {
        "series_id": "PCEPI",
        "source": "FRED",
        "category": "inflation",
        "frequency": "monthly",
        "unit": "index",
        "title": "Personal Consumption Expenditures Price Index",
    },
    {
        "series_id": "UNRATE",
        "source": "FRED",
        "category": "labor",
        "frequency": "monthly",
        "unit": "percent",
        "title": "Unemployment Rate",
    },
    {
        "series_id": "PAYEMS",
        "source": "FRED",
        "category": "labor",
        "frequency": "monthly",
        "unit": "thousands",
        "title": "All Employees, Total Nonfarm",
    },
    {
        "series_id": "RSAFS",
        "source": "FRED",
        "category": "growth",
        "frequency": "monthly",
        "unit": "millions_usd",
        "title": "Advance Retail Sales: Retail and Food Services",
    },
    {
        "series_id": "UMCSENT",
        "source": "FRED",
        "category": "growth",
        "frequency": "monthly",
        "unit": "index",
        "title": "University of Michigan Consumer Sentiment",
    },
    {
        "series_id": "VIXCLS",
        "source": "FRED",
        "category": "volatility",
        "frequency": "daily",
        "unit": "index",
        "title": "CBOE Volatility Index: VIX",
    },
    {
        "series_id": "BAMLC0A0CM",
        "source": "FRED",
        "category": "credit",
        "frequency": "daily",
        "unit": "percent",
        "title": "ICE BofA US Corporate Index Option-Adjusted Spread",
    },
    {
        "series_id": "BAMLH0A0HYM2",
        "source": "FRED",
        "category": "credit",
        "frequency": "daily",
        "unit": "percent",
        "title": "ICE BofA US High Yield Index Option-Adjusted Spread",
    },
]


def registry_by_series_id() -> dict[str, dict[str, str]]:
    return {item["series_id"]: item for item in FRED_SERIES_REGISTRY}
