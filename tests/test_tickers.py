"""Tests for ingestion.tickers using a small temporary CSV."""
import pytest

from ingestion.tickers import DEFAULT_SEED, load_tickers


def test_reads_symbols_and_adds_suffix(tmp_path):
    csv = tmp_path / "list.csv"
    csv.write_text("Company Name,Industry,Symbol,Series,ISIN Code\n"
                   "Reliance Industries Ltd.,Oil Gas & Consumable Fuels,RELIANCE,EQ,INE002A01018\n"
                   "Mahindra & Mahindra Ltd.,Automobile and Auto Components,M&M,EQ,INE101A01026\n"
                   "Reliance Industries Ltd.,Oil Gas & Consumable Fuels,RELIANCE,EQ,INE002A01018\n")
    assert load_tickers(csv) == ["RELIANCE.NS", "M&M.NS"]


def test_missing_symbol_column_raises(tmp_path):
    csv = tmp_path / "bad.csv"
    csv.write_text("Company Name,Industry\nX,Y\n")
    with pytest.raises(ValueError, match="Symbol"):
        load_tickers(csv)


@pytest.mark.skipif(not DEFAULT_SEED.exists(), reason="seed CSV not downloaded")
def test_real_seed_has_50_tickers():
    assert len(load_tickers()) == 50
