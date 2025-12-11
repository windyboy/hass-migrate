"""Unit tests for dependency module."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from hass_migrate.utils.dependency import DependencyAnalyzer


class TestDependencyAnalyzer:
    """Test cases for DependencyAnalyzer class."""

    @pytest.mark.asyncio
    async def test_analyze_dependencies_empty(self):
        """Test dependency analysis with no foreign keys."""
        analyzer = DependencyAnalyzer()
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetch.return_value = []

        result = await analyzer.analyze_dependencies(mock_pool, "hass")

        assert result == {}
        mock_conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_analyze_dependencies_simple(self):
        """Test dependency analysis with simple foreign keys."""
        analyzer = DependencyAnalyzer()
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        # Mock rows: events depends on event_types and event_data
        # Create dict-like objects that support [] access like asyncpg.Record
        class MockRecord(dict):
            """Mock record that behaves like asyncpg.Record."""

            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.__dict__.update(kwargs)

        mock_rows = [
            MockRecord(table_name="events", foreign_table_name="event_types"),
            MockRecord(table_name="events", foreign_table_name="event_data"),
        ]
        mock_conn.fetch.return_value = mock_rows

        result = await analyzer.analyze_dependencies(mock_pool, "hass")

        assert "events" in result
        assert "event_types" in result["events"]
        assert "event_data" in result["events"]

    @pytest.mark.asyncio
    async def test_analyze_dependencies_no_duplicates(self):
        """Test that duplicate dependencies are not added."""
        analyzer = DependencyAnalyzer()
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        # Mock rows with duplicate dependency
        # Create dict-like objects that support [] access like asyncpg.Record
        class MockRecord(dict):
            """Mock record that behaves like asyncpg.Record."""

            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.__dict__.update(kwargs)

        mock_rows = [
            MockRecord(table_name="events", foreign_table_name="event_types"),
            MockRecord(table_name="events", foreign_table_name="event_types"),
        ]
        mock_conn.fetch.return_value = mock_rows

        result = await analyzer.analyze_dependencies(mock_pool, "hass")

        assert "events" in result
        assert len(result["events"]) == 1
        assert result["events"] == ["event_types"]

    @pytest.mark.asyncio
    async def test_analyze_dependencies_self_reference_excluded(self):
        """Test that self-references are excluded from dependencies."""
        analyzer = DependencyAnalyzer()
        mock_pool = MagicMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        # Mock rows: states has self-reference (old_state_id -> states)
        mock_rows = [
            MagicMock(**{"table_name": "states", "foreign_table_name": "states"}),
        ]
        mock_conn.fetch.return_value = mock_rows

        result = await analyzer.analyze_dependencies(mock_pool, "hass")

        # Self-references should be filtered out by the query (ccu.table_name != tc.table_name)
        # But if they somehow get through, they shouldn't be in the result
        assert "states" not in result or "states" not in result.get("states", [])

    def test_topological_sort_no_dependencies(self):
        """Test topological sort with no dependencies."""
        analyzer = DependencyAnalyzer()
        tables = ["event_types", "event_data", "states_meta"]
        dependencies = {}

        result = analyzer.topological_sort(tables, dependencies)

        assert len(result) == 1
        assert set(result[0]) == {"event_types", "event_data", "states_meta"}

    def test_topological_sort_linear_dependencies(self):
        """Test topological sort with linear dependencies."""
        analyzer = DependencyAnalyzer()
        tables = ["event_types", "events"]
        dependencies = {"events": ["event_types"]}

        result = analyzer.topological_sort(tables, dependencies)

        assert len(result) == 2
        assert result[0] == ["event_types"]
        assert result[1] == ["events"]

    def test_topological_sort_complex_dependencies(self):
        """Test topological sort with complex dependencies."""
        analyzer = DependencyAnalyzer()
        tables = ["event_types", "event_data", "events", "states_meta", "states"]
        dependencies = {
            "events": ["event_types", "event_data"],
            "states": ["states_meta", "events"],
        }

        result = analyzer.topological_sort(tables, dependencies)

        # First level should have base tables
        assert "event_types" in result[0]
        assert "event_data" in result[0]
        assert "states_meta" in result[0]

        # Second level should have events
        assert any("events" in level for level in result)

        # States should come after events
        states_level = next(i for i, level in enumerate(result) if "states" in level)
        events_level = next(i for i, level in enumerate(result) if "events" in level)
        assert states_level > events_level

    def test_topological_sort_circular_dependencies(self):
        """Test topological sort raises error on circular dependencies."""
        analyzer = DependencyAnalyzer()
        tables = ["table_a", "table_b"]
        dependencies = {
            "table_a": ["table_b"],
            "table_b": ["table_a"],  # Circular dependency
        }

        with pytest.raises(ValueError, match="Circular dependency detected"):
            analyzer.topological_sort(tables, dependencies)

    def test_get_self_referencing_tables_none(self):
        """Test identifying self-referencing tables when none exist."""
        analyzer = DependencyAnalyzer()
        dependencies = {
            "events": ["event_types"],
            "states": ["events"],
        }

        result = analyzer.get_self_referencing_tables(dependencies)

        assert result == []

    def test_get_self_referencing_tables_some(self):
        """Test identifying self-referencing tables."""
        analyzer = DependencyAnalyzer()
        dependencies = {
            "events": ["event_types"],
            "states": ["states"],  # Self-reference
        }

        result = analyzer.get_self_referencing_tables(dependencies)

        assert "states" in result
        assert "events" not in result
