"""Unit tests for data_cleaner module."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from hass_migrate.utils.data_cleaner import clean_batch_values, clean_value


class TestCleanValue:
    """Test cases for clean_value function."""

    def test_none_value(self):
        """Test that None values remain None."""
        assert clean_value("events", "event_id", None) is None

    def test_string_with_null_bytes(self):
        """Test removal of null bytes from strings."""
        value = "test\x00string\x00with\x00nulls"
        result = clean_value("events", "event_type", value)
        assert result == "teststringwithnulls"
        assert "\x00" not in result

    def test_empty_string_converts_to_none(self):
        """Test that empty strings convert to None."""
        assert clean_value("events", "event_type", "") is None

    def test_boolean_columns_from_int(self):
        """Test conversion of int to bool for boolean columns."""
        # Test recorder_runs.closed_incorrect
        assert clean_value("recorder_runs", "closed_incorrect", 0) is False
        assert clean_value("recorder_runs", "closed_incorrect", 1) is True

        # Test statistics_meta.has_mean
        assert clean_value("statistics_meta", "has_mean", 0) is False
        assert clean_value("statistics_meta", "has_mean", 1) is True

        # Test statistics_meta.has_sum
        assert clean_value("statistics_meta", "has_sum", 0) is False
        assert clean_value("statistics_meta", "has_sum", 1) is True

    def test_boolean_columns_already_bool(self):
        """Test that boolean values remain boolean."""
        assert clean_value("recorder_runs", "closed_incorrect", False) is False
        assert clean_value("recorder_runs", "closed_incorrect", True) is True

    def test_boolean_columns_invalid_int(self, caplog):
        """Test that invalid int values in boolean columns are kept as-is with warning."""
        result = clean_value("recorder_runs", "closed_incorrect", 5)
        assert result == 5
        # Check that warning was logged
        assert "non-boolean integer 5" in caplog.text

    def test_non_boolean_columns_int_not_converted(self):
        """Test that int values in non-boolean columns are not converted."""
        assert clean_value("events", "event_id", 123) == 123
        assert clean_value("states", "state_id", 0) == 0

    def test_datetime_naive_remains_naive(self):
        """Test that naive datetime values remain naive."""
        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = clean_value("events", "time_fired", dt)
        assert result == dt
        assert result.tzinfo is None

    def test_datetime_timezone_aware_converts_to_utc_naive(self):
        """Test that timezone-aware datetime converts to UTC naive."""
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = clean_value("events", "time_fired", dt)
        assert result.tzinfo is None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1

    def test_timestamp_columns_from_float(self):
        """Test conversion of float (Unix timestamp) to datetime for timestamp columns."""
        # Test events.time_fired
        timestamp = 1704110400.0  # 2024-01-01 12:00:00 UTC
        result = clean_value("events", "time_fired", timestamp)
        assert isinstance(result, datetime)
        assert result.tzinfo is None

    def test_timestamp_columns_from_int(self):
        """Test conversion of int (Unix timestamp) to datetime for timestamp columns."""
        timestamp = 1704110400  # 2024-01-01 12:00:00 UTC
        result = clean_value("states", "last_changed", timestamp)
        assert isinstance(result, datetime)
        assert result.tzinfo is None

    def test_timestamp_columns_invalid_timestamp(self, caplog):
        """Test handling of invalid timestamp values."""
        # Test with a value that causes OverflowError (infinity)
        invalid_timestamp = float("inf")
        result = clean_value("events", "time_fired", invalid_timestamp)
        # Should return original value with warning
        assert result == invalid_timestamp
        # Check that warning was logged
        assert "invalid timestamp" in caplog.text

    def test_non_timestamp_columns_float_not_converted(self):
        """Test that float values in non-timestamp columns are not converted."""
        assert clean_value("statistics", "mean", 123.45) == 123.45

    def test_other_types_returned_as_is(self):
        """Test that other types (int, float, bytes) are returned as-is."""
        assert clean_value("events", "event_id", 123) == 123
        assert clean_value("events", "event_id", 123.45) == 123.45
        assert clean_value("events", "event_id", b"binary") == b"binary"


class TestCleanBatchValues:
    """Test cases for clean_batch_values function."""

    def test_clean_batch_normal(self):
        """Test cleaning a batch of normal rows."""
        table = "events"
        columns = ["event_id", "event_type", "time_fired"]
        rows = [
            (1, "state_changed", datetime(2024, 1, 1, 12, 0, 0)),
            (2, "test\x00with\x00nulls", None),
            (3, "", datetime(2024, 1, 1, 13, 0, 0)),
        ]
        result = clean_batch_values(table, columns, rows)
        assert len(result) == 3
        assert result[0] == [1, "state_changed", datetime(2024, 1, 1, 12, 0, 0)]
        assert result[1] == [2, "testwithnulls", None]
        assert result[2] == [3, None, datetime(2024, 1, 1, 13, 0, 0)]

    def test_clean_batch_with_boolean_column(self):
        """Test cleaning a batch with boolean columns."""
        table = "recorder_runs"
        columns = ["run_id", "closed_incorrect"]
        rows = [
            (1, 0),
            (2, 1),
            (3, True),
        ]
        result = clean_batch_values(table, columns, rows)
        assert len(result) == 3
        assert result[0] == [1, False]
        assert result[1] == [2, True]
        assert result[2] == [3, True]

    def test_clean_batch_mismatched_column_count(self):
        """Test that rows with mismatched column counts are skipped."""
        table = "events"
        columns = ["event_id", "event_type"]
        rows = [
            (1, "test"),
            (2,),  # Missing column
            (3, "test", "extra"),  # Extra column
        ]
        result = clean_batch_values(table, columns, rows)
        # Only the first row should be included
        assert len(result) == 1
        assert result[0] == [1, "test"]

    def test_clean_batch_empty(self):
        """Test cleaning an empty batch."""
        table = "events"
        columns = ["event_id", "event_type"]
        rows = []
        result = clean_batch_values(table, columns, rows)
        assert result == []
