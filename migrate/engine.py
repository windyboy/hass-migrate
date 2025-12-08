from __future__ import annotations

import json
import os
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import mysql.connector
import asyncpg

DEFAULT_BATCH_SIZE = 20_000
PROGRESS_FILE = "migration_progress.json"

# 按“差异清单”列出需要 int→bool 的字段（表名 + 列名）
BOOL_COLUMNS = {
    ("recorder_runs", "closed_incorrect"),
    ("statistics_meta", "has_mean"),
    ("statistics_meta", "has_sum"),
}


def clean_value(table: str, column: str, value: Any) -> Any:
    """
    统一清洗从 MySQL 读出来的值，确保 PostgreSQL 能接受。

    差异处理规则：

    1. NULL 保持 NULL
    2. 字符串:
       - 去掉 \x00
       - 空字符串 "" → None（特别是时间字段 from CHAR(0)）
    3. 按差异清单，对特定字段执行 int(0/1) → bool
    4. datetime → 补上 tzinfo=UTC（HA 逻辑上使用 UTC）
    5. 其它类型原样返回
    """
    # 1. NULL 原样返回
    if value is None:
        return None

    # 2. 针对字符串：去除 null 字节 + 空串→NULL
    if isinstance(value, str):
        if "\x00" in value:
            value = value.replace("\x00", "")
        if value == "":
            return None
        return value

    # 3. 只对“明确是布尔字段”的 tinyint 做 0/1→bool
    if (table, column) in BOOL_COLUMNS:
        # MySQL 驱动一般给 tinyint(1) 返回 int
        if isinstance(value, bool):
            return value
        if isinstance(value, int):
            # 只接受 0 和 1，其他值直接报错也比默默乱转好
            if value in (0, 1):
                return bool(value)
        # 理论上不会走到这里，如果走到了就是数据异常
        return value

    # 4. datetime → 补充时区信息
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        # 统一转换为 UTC，避免时区混乱
        return value.astimezone(timezone.utc)

    # 5. 其它类型（int、float、bytes、等）原样返回
    return value


class Migrator:
    def __init__(self, cfg, batch_size: int = DEFAULT_BATCH_SIZE):
        self.cfg = cfg
        self.batch_size = batch_size
        self.mysql: Optional[mysql.connector.MySQLConnection] = None
        self.pg: Optional[asyncpg.Connection] = None
        self.progress: Dict[str, Dict[str, Any]] = {}

    def load_progress(self) -> None:
        """Load migration progress from file."""
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, "r") as f:
                self.progress = json.load(f)

    def save_progress(self) -> None:
        """Save migration progress to file."""
        with open(PROGRESS_FILE, "w") as f:
            json.dump(self.progress, f, indent=2)

    # ---------- Connections ----------

    def connect_mysql(self) -> None:
        self.mysql = mysql.connector.connect(
            host=self.cfg.mysql_host,
            port=self.cfg.mysql_port,
            user=self.cfg.mysql_user,
            password=self.cfg.mysql_password,
            database=self.cfg.mysql_db,
            charset="utf8mb4",
        )

    async def connect_pg(self) -> None:
        self.pg = await asyncpg.connect(
            user=self.cfg.pg_user,
            password=self.cfg.pg_password,
            database=self.cfg.pg_db,
            host=self.cfg.pg_host,
            port=self.cfg.pg_port,
        )

    async def close(self) -> None:
        if self.pg is not None:
            await self.pg.close()
            self.pg = None
        if self.mysql is not None:
            self.mysql.close()
            self.mysql = None

    # ---------- Schema helpers ----------

    async def schema_exists(self) -> bool:
        assert self.pg is not None
        count = await self.pg.fetchval(
            """
            SELECT COUNT(*)
            FROM pg_tables
            WHERE schemaname = 'public'
            """
        )
        return count > 0

    async def apply_schema(self, filename: str) -> None:
        assert self.pg is not None
        with open(filename, "r", encoding="utf-8") as f:
            sql = f.read()
        await self.pg.execute(sql)

    async def truncate_table(self, table: str) -> None:
        assert self.pg is not None
        # RESTART IDENTITY 重置序列，CASCADE 处理外键依赖
        await self.pg.execute(
            f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE;'
        )

    # ---------- Data migration ----------

    async def migrate_table(self, table: str, columns: List[str]) -> None:
        """
        通用表迁移逻辑：
        - 从 MySQL SELECT 对应列，支持断点续传
        - 逐行按差异清单清洗数据
        - 批量 INSERT 到 PostgreSQL
        """
        assert self.mysql is not None
        assert self.pg is not None

        pk_col = columns[0]  # 假设第一列是主键
        last_id = self.progress.get(table, {}).get("last_id", None)
        total_migrated = self.progress.get(table, {}).get("total", 0)

        cursor = self.mysql.cursor()

        # MySQL 侧列名
        col_str = ", ".join(columns)
        if last_id is not None:
            cursor.execute(f"SELECT {col_str} FROM {table} WHERE {pk_col} > %s ORDER BY {pk_col}", (last_id,))
        else:
            cursor.execute(f"SELECT {col_str} FROM {table} ORDER BY {pk_col}")

        # PostgreSQL 侧
        pg_columns = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(f"${i+1}" for i in range(len(columns)))
        insert_sql = f'INSERT INTO "{table}" ({pg_columns}) VALUES ({placeholders})'

        total = total_migrated

        while True:
            rows: Sequence[Sequence[Any]] = cursor.fetchmany(self.batch_size)
            if not rows:
                break

            # 按列名逐个清洗
            cleaned_batch = [
                [clean_value(table, col, val) for col, val in zip(columns, row)]
                for row in rows
            ]

            # 重试机制
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await self.pg.executemany(insert_sql, cleaned_batch)
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    print(f"Retry {attempt + 1}/{max_retries} for {table} batch: {e}")
                    await asyncio.sleep(1)

            total += len(cleaned_batch)

            # 更新进度
            last_id = rows[-1][0]  # 假设第一列是ID
            self.progress[table] = {"last_id": last_id, "total": total}
            self.save_progress()

            print(f"{table}: {total:,} rows migrated...")

        cursor.close()

    async def fix_sequence(self, table: str, pk: str) -> None:
        """
        把 PostgreSQL 对应的序列调整到 MAX(pk)，避免后续插入主键冲突。
        """
        assert self.pg is not None

        seq = await self.pg.fetchval(
            "SELECT pg_get_serial_sequence($1, $2)", table, pk
        )
        if not seq:
            # 例如 migration_changes.migration_id 这种 varchar PK 没有序列
            return

        await self.pg.execute(
            f"SELECT setval($1, (SELECT COALESCE(MAX({pk}), 1) FROM {table}))",
            seq,
        )

