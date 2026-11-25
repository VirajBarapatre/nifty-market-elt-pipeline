"""Validate fetched prices and split them into good rows and rejected rows.

Rejected rows are kept (not dropped) with a reason, so we can inspect them
later. In Milestone 6 they go to a quarantine table.
"""
import pandas as pd

REQUIRED_COLUMNS = ["ticker", "trade_date", "open", "high", "low", "close", "adj_close", "volume"]
PRICE_COLUMNS = ["open", "high", "low", "close", "adj_close"]


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (good_df, rejected_df). rejected_df has an extra reject_reason column."""
    # 1. Schema: if a column is missing, the whole batch is broken, not one row.
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = df.reset_index(drop=True)
    reasons = pd.Series("", index=df.index)

    def flag(mask: pd.Series, reason: str) -> None:
        # Append the reason to every row where mask is True (a row can fail several checks).
        reasons[mask] = reasons[mask] + reason + ";"

    # 2. Nulls in any required column.
    flag(df[REQUIRED_COLUMNS].isna().any(axis=1), "null_value")

    # 3. Prices must be positive. (NaN <= 0 is False, so nulls are only flagged once.)
    flag((df[PRICE_COLUMNS] <= 0).any(axis=1), "non_positive_price")

    # 4. Volume: negative is impossible; zero means no trading happened that day.
    flag(df["volume"] < 0, "negative_volume")
    flag(df["volume"] == 0, "zero_volume")

    # 5. High must be >= low, and open/close must sit inside [low, high].
    flag(df["high"] < df["low"], "high_below_low")
    flag((df["open"] > df["high"]) | (df["open"] < df["low"])
         | (df["close"] > df["high"]) | (df["close"] < df["low"]), "ohlc_out_of_range")

    # 6. Duplicate ticker + date: we can't know which copy is right, so reject all copies.
    flag(df.duplicated(subset=["ticker", "trade_date"], keep=False), "duplicate_ticker_date")

    is_bad = reasons != ""
    good = df[~is_bad].copy()
    rejected = df[is_bad].copy()
    rejected["reject_reason"] = reasons[is_bad].str.rstrip(";")
    return good, rejected
