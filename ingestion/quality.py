"""Per-run data quality metrics and gates. Pure pandas: no BigQuery, easy to unit test.

Two kinds of gate:
- HARD gates block the load. If a large share of the batch is bad, something is wrong
  with the source or the file, and loading even the "good" rows is risky. Fail closed.
- SOFT gates let the load happen, then fail the run so someone looks at it.
  Missing tickers are soft: the rows we did get are fine, and the next run's
  7-day re-fetch usually fills the gap.
"""
from dataclasses import asdict, dataclass, field
from datetime import date

import pandas as pd

PRICE_COLUMNS = ["open", "high", "low", "close", "adj_close", "volume"]

MAX_REJECT_RATE = 0.10   # hard: more than 10% of rows rejected -> don't load
MAX_NULL_RATE = 0.05     # hard: more than 5% of price cells null -> don't load
MAX_MISSING_TICKERS = 0  # soft: any expected ticker missing on the trade date -> alert


@dataclass
class RunMetrics:
    run_id: str
    trade_date: date
    rows_fetched: int
    rows_good: int
    rows_rejected: int
    reject_rate: float
    null_rate: float
    tickers_expected: int
    tickers_received: int
    missing_tickers: list[str] = field(default_factory=list)

    def as_row(self) -> dict:
        row = asdict(self)
        row["missing_tickers"] = ",".join(self.missing_tickers)  # one STRING column in BigQuery
        return row


def compute_metrics(run_id: str, trade_date: date, fetched: pd.DataFrame, good: pd.DataFrame,
                    rejected: pd.DataFrame, expected_tickers: list[str]) -> RunMetrics:
    rows = len(fetched)
    cells = fetched.reindex(columns=PRICE_COLUMNS)
    null_rate = float(cells.isna().to_numpy().mean()) if rows else 0.0

    # Completeness is measured on the run's own trade date, using rows that passed validation.
    on_day = good[good["trade_date"] == trade_date] if not good.empty else good
    received = set(on_day["ticker"]) if not on_day.empty else set()
    missing = sorted(set(expected_tickers) - received)

    return RunMetrics(
        run_id=run_id,
        trade_date=trade_date,
        rows_fetched=rows,
        rows_good=len(good),
        rows_rejected=len(rejected),
        reject_rate=round(len(rejected) / rows, 4) if rows else 0.0,
        null_rate=round(null_rate, 4),
        tickers_expected=len(expected_tickers),
        tickers_received=len(received),
        missing_tickers=missing,
    )


def hard_gate_failures(m: RunMetrics) -> list[str]:
    """Problems that must block the load."""
    problems = []
    if m.rows_fetched == 0:
        problems.append("no rows fetched")
    if m.reject_rate > MAX_REJECT_RATE:
        problems.append(f"reject rate {m.reject_rate:.1%} > {MAX_REJECT_RATE:.0%}")
    if m.null_rate > MAX_NULL_RATE:
        problems.append(f"null rate {m.null_rate:.1%} > {MAX_NULL_RATE:.0%}")
    return problems


def soft_gate_failures(m: RunMetrics) -> list[str]:
    """Problems that allow the load but should fail the run as an alert."""
    if len(m.missing_tickers) > MAX_MISSING_TICKERS:
        return [f"{len(m.missing_tickers)} of {m.tickers_expected} tickers missing on "
                f"{m.trade_date}: {', '.join(m.missing_tickers)}"]
    return []


def run_log_row(m: RunMetrics, status: str, problems: list[str], rows_loaded: int = 0) -> dict:
    """The row written to raw.pipeline_runs for this run."""
    return {**m.as_row(), "status": status, "problems": " | ".join(problems),
            "rows_loaded": rows_loaded}
