import os
from typing import List, Tuple

from rich.console import Console

from migrate.utils.logger import StructuredLogger

# Path to schema file relative to this file's directory
SCHEMA_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schema", "postgres_schema.sql")
PROGRESS_FILE = "migration_progress.json"

# Initialize shared objects
console = Console()
logger = StructuredLogger("migrate")

# 注意：这里每个 tuple 是 (表名, [字段列表])
TABLES: List[Tuple[str, List[str]]] = [
    # 先迁 event 相关基础表
    ("event_types", ["event_type_id", "event_type"]),
    ("event_data", ["data_id", "hash", "shared_data"]),
    (
        "events",
        [
            "event_id",
            "event_type",
            "event_data",
            "origin",
            "origin_idx",
            "time_fired",
            "time_fired_ts",
            "context_id",
            "context_user_id",
            "context_parent_id",
            "data_id",
            "context_id_bin",
            "context_user_id_bin",
            "context_parent_id_bin",
            "event_type_id",
        ],
    ),
    # states 相关
    ("state_attributes", ["attributes_id", "hash", "shared_attrs"]),
    ("states_meta", ["metadata_id", "entity_id"]),
    (
        "states",
        [
            "state_id",
            "entity_id",
            "state",
            "attributes",
            "event_id",
            "last_changed",
            "last_changed_ts",
            "last_reported_ts",
            "last_updated",
            "last_updated_ts",
            "old_state_id",
            "attributes_id",
            "context_id",
            "context_user_id",
            "context_parent_id",
            "origin_idx",
            "context_id_bin",
            "context_user_id_bin",
            "context_parent_id_bin",
            "metadata_id",
        ],
    ),
    # 统计相关
    (
        "statistics_meta",
        [
            "id",
            "statistic_id",
            "source",
            "unit_of_measurement",
            "unit_class",
            "has_mean",
            "has_sum",
            "name",
            "mean_type",
        ],
    ),
    (
        "statistics",
        [
            "id",
            "created",
            "created_ts",
            "metadata_id",
            "start",
            "start_ts",
            "mean",
            "mean_weight",
            "min",
            "max",
            "last_reset",
            "last_reset_ts",
            "state",
            "sum",
        ],
    ),
    (
        "statistics_short_term",
        [
            "id",
            "created",
            "created_ts",
            "metadata_id",
            "start",
            "start_ts",
            "mean",
            "mean_weight",
            "min",
            "max",
            "last_reset",
            "last_reset_ts",
            "state",
            "sum",
        ],
    ),
    # runs / schema / migration
    ("recorder_runs", ["run_id", "start", "end", "closed_incorrect", "created"]),
    ("statistics_runs", ["run_id", "start"]),
    ("schema_changes", ["change_id", "schema_version", "changed"]),
    ("migration_changes", ["migration_id", "version"]),
]

