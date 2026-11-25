"""Read the ticker list from the official Nifty 50 constituents CSV.

The CSV is downloaded from niftyindices.com and kept in dbt/seeds/, so the
ingestion code and dbt (Milestone 3) share ONE source of truth for tickers.
"""
from pathlib import Path

import pandas as pd

DEFAULT_SEED = Path(__file__).resolve().parent.parent / "dbt" / "seeds" / "ind_nifty50list.csv"


def load_tickers(path: Path = DEFAULT_SEED, suffix: str = ".NS") -> list[str]:
    """Return Yahoo tickers like 'RELIANCE.NS' from the NSE 'Symbol' column."""
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    if "Symbol" not in df.columns:
        raise ValueError(f"{path} has no 'Symbol' column; columns are {list(df.columns)}")
    symbols = df["Symbol"].dropna().astype(str).str.strip()
    symbols = symbols[symbols != ""].drop_duplicates()
    return [f"{s}{suffix}" for s in symbols]
