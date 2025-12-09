"""Backup service for database backups."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from migrate.config import DBConfig
from migrate.utils.logger import StructuredLogger


class BackupService:
    """Service for creating and restoring database backups."""

    def __init__(self, logger: StructuredLogger):
        """
        Initialize backup service.

        Args:
            logger: Logger instance
        """
        self.logger = logger

    async def create_backup(
        self, config: DBConfig, backup_dir: str = "backups"
    ) -> str:
        """
        Create a PostgreSQL database backup using pg_dump.

        Args:
            config: Database configuration
            backup_dir: Directory to store backups

        Returns:
            Path to backup file
        """
        # Create backup directory if it doesn't exist
        Path(backup_dir).mkdir(parents=True, exist_ok=True)

        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"hass_backup_{timestamp}.dump"
        backup_path = os.path.join(backup_dir, backup_filename)

        # Set PGPASSWORD environment variable
        env = os.environ.copy()
        env["PGPASSWORD"] = config.pg_password

        # Build pg_dump command
        cmd = [
            "pg_dump",
            f"--host={config.pg_host}",
            f"--port={config.pg_port}",
            f"--username={config.pg_user}",
            f"--dbname={config.pg_db}",
            "--format=custom",
            "--file",
            backup_path,
        ]

        self.logger.info(f"Creating backup: {backup_path}")

        # Execute pg_dump
        process = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            raise RuntimeError(f"Backup failed: {error_msg}")

        self.logger.info(f"Backup created successfully: {backup_path}")
        return backup_path

    async def restore_backup(
        self, config: DBConfig, backup_path: str
    ) -> None:
        """
        Restore a PostgreSQL database backup using pg_restore.

        Args:
            config: Database configuration
            backup_path: Path to backup file
        """
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Backup file not found: {backup_path}")

        # Set PGPASSWORD environment variable
        env = os.environ.copy()
        env["PGPASSWORD"] = config.pg_password

        # Build pg_restore command
        cmd = [
            "pg_restore",
            f"--host={config.pg_host}",
            f"--port={config.pg_port}",
            f"--username={config.pg_user}",
            f"--dbname={config.pg_db}",
            "--clean",
            "--if-exists",
            backup_path,
        ]

        self.logger.info(f"Restoring backup: {backup_path}")

        # Execute pg_restore
        process = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            error_msg = stderr.decode() if stderr else "Unknown error"
            raise RuntimeError(f"Restore failed: {error_msg}")

        self.logger.info("Backup restored successfully")

