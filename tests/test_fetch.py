"""Tests for ingestion.fetch. yf.download is replaced with a fake: unit tests must
not call the internet (slow, flaky, rate limited)."""
from datetime import date

import pandas as pd

from ingestion import fetch


def fake_yf_frame():
    idx = pd.DatetimeIndex(["2026-11-02", "2026-11-03"], name="Date")
    return pd.DataFrame({
        "Open": [100.0, 101.0], "High": [110.0, 111.0], "Low": [95.0, 96.0],
        "Close": [105.0, 106.0], "Adj Close": [104.0, 105.0], "Volume": [1000, 2000],
    }, index=idx)


def test_fetch_prices_shapes_columns(monkeypatch):
    monkeypatch.setattr(fetch.yf, "download", lambda *a, **k: fake_yf_frame())
    df = fetch.fetch_prices(["RELIANCE.NS"], date(2026, 11, 1), date(2026, 11, 4), pause_seconds=0)
    assert list(df.columns) == fetch.OUTPUT_COLUMNS
    assert len(df) == 2
    assert df["ticker"].unique().tolist() == ["RELIANCE.NS"]
    assert df["trade_date"].iloc[0] == date(2026, 11, 2)


def test_bad_ticker_is_skipped_not_fatal(monkeypatch):
    def fake_download(ticker, **kwargs):
        return pd.DataFrame() if ticker == "NOTAREAL.NS" else fake_yf_frame()
    monkeypatch.setattr(fetch.yf, "download", fake_download)
    monkeypatch.setattr(fetch.time, "sleep", lambda s: None)  # skip backoff waits
    df = fetch.fetch_prices(["NOTAREAL.NS", "TCS.NS"], date(2026, 11, 1), date(2026, 11, 4))
    assert df["ticker"].unique().tolist() == ["TCS.NS"]


def test_retries_then_succeeds(monkeypatch):
    calls = {"n": 0}

    def flaky_download(ticker, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("rate limited")
        return fake_yf_frame()

    monkeypatch.setattr(fetch.yf, "download", flaky_download)
    monkeypatch.setattr(fetch.time, "sleep", lambda s: None)
    df = fetch.fetch_one_ticker("INFY.NS", date(2026, 11, 1), date(2026, 11, 4))
    assert calls["n"] == 3
    assert len(df) == 2


def test_all_fail_returns_empty_frame_with_columns(monkeypatch):
    monkeypatch.setattr(fetch.yf, "download", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(fetch.time, "sleep", lambda s: None)
    df = fetch.fetch_prices(["X.NS"], date(2026, 11, 1), date(2026, 11, 4))
    assert df.empty and list(df.columns) == fetch.OUTPUT_COLUMNS
