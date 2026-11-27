"""Tests for ingestion.audit.extend_expiry with a fake BigQuery client (no network)."""
import copy
from datetime import datetime, timezone

import pandas as pd
from google.api_core.exceptions import Forbidden

from ingestion import audit


class FakeTable:
    def __init__(self):
        self.expires = datetime(2027, 1, 1, tzinfo=timezone.utc)


class FakeClient:
    def __init__(self, refuse: bool):
        self.refuse = refuse
        self.table = FakeTable()

    def get_table(self, table_id):
        return copy.copy(self.table)

    def update_table(self, table, fields):
        if self.refuse:
            raise Forbidden("Table expiration time must be less than 60 days while in sandbox mode")
        self.table = table
        return table


def test_extend_expiry_moves_the_date_forward():
    client = FakeClient(refuse=False)
    table = audit.extend_expiry(client, "p.d.t")
    days = (table.expires - datetime.now(timezone.utc)).days
    assert audit.EXPIRY_DAYS - 1 <= days <= audit.EXPIRY_DAYS


def test_extend_expiry_refused_does_not_raise(caplog):
    client = FakeClient(refuse=True)
    table = audit.extend_expiry(client, "p.d.t")
    assert table.expires == datetime(2027, 1, 1, tzinfo=timezone.utc)
    assert "ALERT" in caplog.text


def test_write_rejected_with_no_rows_still_extends_expiry():
    client = FakeClient(refuse=False)
    assert audit.write_rejected(pd.DataFrame(), "run", client, "p", "d") == 0
    assert (client.table.expires - datetime.now(timezone.utc)).days >= audit.EXPIRY_DAYS - 1
