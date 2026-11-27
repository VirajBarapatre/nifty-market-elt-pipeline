"""Tests for ingestion.quality: metrics and the hard/soft gates. No BigQuery needed."""
from datetime import date
from pathlib import Path

import pandas as pd

from ingestion.quality import compute_metrics, hard_gate_failures, run_log_row, soft_gate_failures
from ingestion.run import read_input_file
from ingestion.validate import validate

DAY = date(2026, 11, 26)
EXPECTED = ["RELIANCE.NS", "TCS.NS", "INFY.NS"]
FIXTURE = Path(__file__).parent / "fixtures" / "bad_prices.csv"


def frame(tickers, **overrides):
    rows = [dict(ticker=t, trade_date=DAY, open=100.0, high=110.0, low=95.0, close=105.0,
                 adj_close=105.0, volume=1_000) for t in tickers]
    for r in rows:
        r.update(overrides)
    return pd.DataFrame(rows)


def metrics_for(df, expected=EXPECTED):
    good, rejected = validate(df, allowed_tickers=expected)
    return compute_metrics("test", DAY, df, good, rejected, expected)


def test_clean_batch_passes_every_gate():
    m = metrics_for(frame(EXPECTED))
    assert (m.rows_good, m.rows_rejected, m.reject_rate, m.null_rate) == (3, 0, 0.0, 0.0)
    assert hard_gate_failures(m) == []
    assert soft_gate_failures(m) == []


def test_empty_batch_is_a_hard_failure():
    m = metrics_for(frame([]).reindex(columns=frame(EXPECTED).columns))
    assert "no rows fetched" in hard_gate_failures(m)


def test_high_reject_rate_blocks_the_load():
    m = metrics_for(frame(EXPECTED, volume=0))          # every row is zero_volume
    assert m.reject_rate == 1.0
    assert any("reject rate" in p for p in hard_gate_failures(m))


def test_null_rate_counts_cells_not_rows():
    m = metrics_for(frame(EXPECTED, close=None))        # 3 null cells out of 3 x 6
    assert m.null_rate == round(3 / 18, 4)
    assert any("null rate" in p for p in hard_gate_failures(m))


def test_missing_ticker_is_soft_not_hard():
    m = metrics_for(frame(["RELIANCE.NS", "TCS.NS"]))
    assert m.missing_tickers == ["INFY.NS"]
    assert hard_gate_failures(m) == []
    assert "INFY.NS" in soft_gate_failures(m)[0]


def test_a_rejected_row_does_not_count_as_received():
    df = frame(EXPECTED)
    df.loc[df["ticker"] == "TCS.NS", "volume"] = 0
    assert metrics_for(df).missing_tickers == ["TCS.NS"]


def test_run_log_row_flattens_lists():
    m = metrics_for(frame(["RELIANCE.NS"]))
    row = run_log_row(m, "loaded_with_warnings", ["a", "b"], rows_loaded=1)
    assert row["missing_tickers"] == "INFY.NS,TCS.NS"
    assert row["problems"] == "a | b"
    assert row["status"] == "loaded_with_warnings"


def test_bad_fixture_is_blocked_before_load():
    """The demo file: 1 good row, 7 bad ones. The hard gate must stop it."""
    df = read_input_file(FIXTURE)
    good, rejected = validate(df, allowed_tickers=EXPECTED + ["HDFCBANK.NS", "ITC.NS", "SBIN.NS"])
    assert list(good["ticker"]) == ["RELIANCE.NS"]
    reasons = ";".join(rejected["reject_reason"])
    for reason in ["non_positive_price", "high_below_low", "null_value",
                   "duplicate_ticker_date", "unknown_ticker", "zero_volume"]:
        assert reason in reasons
    m = compute_metrics("test", DAY, df, good, rejected, EXPECTED)
    assert any("reject rate" in p for p in hard_gate_failures(m))
