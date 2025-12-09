"""Dependency analysis for table migration order."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

import asyncpg
from asyncpg import Pool


class DependencyAnalyzer:
    """Automatically analyze foreign key dependencies between tables."""

    async def analyze_dependencies(
        self, pool: Pool, schema: str = "hass"
    ) -> Dict[str, List[str]]:
        """
        Analyze foreign key dependencies.

        Args:
            pool: PostgreSQL connection pool
            schema: Schema name to analyze

        Returns:
            Dictionary mapping table name to list of dependent table names
        """
        async with pool.acquire() as conn:
            query = """
            SELECT
                tc.table_name,
                ccu.table_name AS foreign_table_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = $1
                AND ccu.table_name != tc.table_name
            ORDER BY tc.table_name, ccu.table_name
            """
            rows = await conn.fetch(query, schema)

            deps = defaultdict(list)
            for row in rows:
                table = row["table_name"]
                foreign_table = row["foreign_table_name"]
                # Avoid duplicates
                if foreign_table not in deps[table]:
                    deps[table].append(foreign_table)

            return dict(deps)

    def topological_sort(
        self, tables: List[str], dependencies: Dict[str, List[str]]
    ) -> List[List[str]]:
        """
        Topological sort to determine migration order.

        Args:
            tables: List of all table names
            dependencies: Dictionary mapping table to its dependencies

        Returns:
            List of levels, where each level contains tables that can be migrated in parallel
        """
        # Calculate in-degree for each table
        in_degree: Dict[str, int] = {t: 0 for t in tables}
        for table, deps in dependencies.items():
            if table in in_degree:
                in_degree[table] = len(deps)

        levels: List[List[str]] = []
        remaining = set(tables)

        while remaining:
            # Find all tables with in-degree 0 (no dependencies or all dependencies satisfied)
            level = [t for t in remaining if in_degree.get(t, 0) == 0]

            if not level:
                # Circular dependency detected - cannot determine safe migration order
                remaining_tables = list(remaining)
                raise ValueError(
                    f"Circular dependency detected among tables: {remaining_tables}. "
                    "Cannot determine safe migration order. Please check foreign key relationships."
                )

            levels.append(level)
            remaining -= set(level)

            # Update in-degree for tables that depend on tables in this level
            for table in level:
                for dep_table, deps in dependencies.items():
                    if dep_table in remaining and table in deps:
                        in_degree[dep_table] = max(0, in_degree.get(dep_table, 0) - 1)

        return levels

    def get_self_referencing_tables(
        self, dependencies: Dict[str, List[str]]
    ) -> List[str]:
        """
        Identify tables with self-referencing foreign keys.

        Args:
            dependencies: Dictionary mapping table to its dependencies

        Returns:
            List of table names that have self-references
        """
        self_refs = []
        for table, deps in dependencies.items():
            if table in deps:
                self_refs.append(table)
        return self_refs
