from __future__ import annotations

FACTOR_DATASET_REGISTRY: list[dict[str, object]] = [
    {
        "dataset_id": "KEN_FRENCH_FF5_DAILY",
        "title": "Fama-French 5 Factors (2x3) Daily",
        "model": "ff5_plus_momentum_us",
        "frequency": "daily",
        "source": "Kenneth French Data Library",
        "url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip",
        "factors": ["MKT_RF", "SMB", "HML", "RMW", "CMA", "RF"],
    },
    {
        "dataset_id": "KEN_FRENCH_FF5_MONTHLY",
        "title": "Fama-French 5 Factors (2x3) Monthly",
        "model": "ff5_plus_momentum_us",
        "frequency": "monthly",
        "source": "Kenneth French Data Library",
        "url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip",
        "factors": ["MKT_RF", "SMB", "HML", "RMW", "CMA", "RF"],
    },
    {
        "dataset_id": "KEN_FRENCH_MOMENTUM_DAILY",
        "title": "Momentum Factor Daily",
        "model": "ff5_plus_momentum_us",
        "frequency": "daily",
        "source": "Kenneth French Data Library",
        "url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip",
        "factors": ["MOM"],
    },
    {
        "dataset_id": "KEN_FRENCH_MOMENTUM_MONTHLY",
        "title": "Momentum Factor Monthly",
        "model": "ff5_plus_momentum_us",
        "frequency": "monthly",
        "source": "Kenneth French Data Library",
        "url": "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_CSV.zip",
        "factors": ["MOM"],
    },
]

FACTOR_DATASET_BY_ID = {
    str(item["dataset_id"]): item for item in FACTOR_DATASET_REGISTRY
}

CORE_FACTOR_DATASETS = {
    "daily": ["KEN_FRENCH_FF5_DAILY", "KEN_FRENCH_MOMENTUM_DAILY"],
    "monthly": ["KEN_FRENCH_FF5_MONTHLY", "KEN_FRENCH_MOMENTUM_MONTHLY"],
}

FACTOR_DISPLAY_NAMES = {
    "MKT_RF": "Market Excess",
    "SMB": "Size",
    "HML": "Value vs Growth",
    "RMW": "Profitability",
    "CMA": "Investment",
    "MOM": "Momentum",
    "RF": "Risk-Free",
}

FACTOR_BETA_COLUMNS = {
    "MKT_RF": "factor_market_beta",
    "SMB": "factor_size_beta",
    "HML": "factor_value_beta",
    "RMW": "factor_profitability_beta",
    "CMA": "factor_investment_beta",
    "MOM": "factor_momentum_beta",
}

CORE_FACTOR_ORDER = ["MKT_RF", "SMB", "HML", "RMW", "CMA", "MOM"]
