from __future__ import annotations

from datetime import datetime, timezone

from foundry_dev_tools.utils.misc import is_dataset_a_view, parse_iso


def test_is_dataset_a_view():
    assert is_dataset_a_view({"record": {"view": True}})
    assert not is_dataset_a_view({"record": {"view": False}})
    assert not is_dataset_a_view({"transaction": {"rid": "", "record": {"view": True}}})
    assert not is_dataset_a_view({"view": True})


def test_parse_iso():
    dt = datetime(year=2024, month=8, day=12, hour=12, minute=38, second=21, tzinfo=timezone.utc)
    palantir_s3_iso_format = "2024-08-12T12:38:21.702342115Z"
    assert parse_iso(palantir_s3_iso_format) == dt
    without_microseconds = "2024-08-12T12:38:21+00:00"
    assert parse_iso(without_microseconds) == dt
    with_microseconds = "2024-08-12T12:38:21.000000+00:00"
    assert parse_iso(with_microseconds) == dt
