"""Tests for ingestion.validate: every rule gets a row that breaks it."""
from datetime import date

import pandas as pd
import pytest

from ingestion.validate import validate

D1 = date(2026, 11, 2)
D2 = date(2026, 11, 3)


def row(**overrides):
    """A valid row; override fields to make it invalid in one specific way."""
    base = dict(ticker="RELIANCE.NS", trade_date=D1, open=100.0, high=110.0, low=95.0,
                close=105.0, adj_close=104.0, volume=1_000)
    base.update(overrides)
    return base


def reasons_for(rows):
    _, rejected = validate(pd.DataFrame(rows))
    return list(rejected["reject_reason"])


def test_valid_row_passes():
    good, rejected = validate(pd.DataFrame([row()]))
    assert len(good) == 1
    assert rejected.empty


def test_missing_column_fails_whole_batch():
    df = pd.DataFrame([row()]).drop(columns=["close"])
    with pytest.raises(ValueError, match="close"):
        validate(df)


@pytest.mark.parametrize("overrides, expected", [
    (dict(open=None), "null_value"),
    (dict(close=0.0, low=0.0), "non_positive_price"),
    (dict(adj_close=-1.0), "non_positive_price"),
    (dict(volume=-5), "negative_volume"),
    (dict(volume=0), "zero_volume"),
    (dict(high=90.0, low=95.0, open=92.0, close=92.0), "high_below_low"),
    (dict(close=120.0), "ohlc_out_of_range"),
])
def test_each_rule_rejects(overrides, expected):
    assert expected in reasons_for([row(**overrides)])[0]


def test_duplicate_ticker_date_rejects_every_copy():
    reasons = reasons_for([row(), row(), row(trade_date=D2)])
    assert reasons == ["duplicate_ticker_date", "duplicate_ticker_date"]


def test_same_date_different_tickers_is_not_a_duplicate():
    good, rejected = validate(pd.DataFrame([row(), row(ticker="TCS.NS")]))
    assert len(good) == 2 and rejected.empty


def test_row_can_fail_several_rules():
    reason = reasons_for([row(high=90.0, volume=0)])[0]
    assert "zero_volume" in reason and "high_below_low" in reason


def test_good_and_rejected_add_up():
    rows = [row(), row(trade_date=D2, volume=0), row(ticker="TCS.NS", open=None)]
    good, rejected = validate(pd.DataFrame(rows))
    assert len(good) + len(rejected) == len(rows)
