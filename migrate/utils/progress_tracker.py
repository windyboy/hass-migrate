"""Progress tracking with optimized IO."""

from __future__ import annotations

import time
from typing import Any, Dict


class ProgressTracker:
    """Optimized progress tracker that reduces file IO."""

    def __init__(
        self,
        update_interval: int = 10,
        min_update_interval_seconds: float = 5.0,
    ):
        """
        Initialize progress tracker.

        Args:
            update_interval: Update progress every N batches
            min_update_interval_seconds: Minimum time between updates (seconds)
        """
        self.update_interval = update_interval
        self.min_update_interval_seconds = min_update_interval_seconds
        self.batch_count = 0
        self.last_update_time = time.time()

    def should_update(self) -> bool:
        """
        Determine if progress should be updated.

        Returns:
            True if progress should be updated
        """
        self.batch_count += 1
        now = time.time()

        # Update if we've reached the batch interval or time interval
        if (
            self.batch_count % self.update_interval == 0
            or now - self.last_update_time >= self.min_update_interval_seconds
        ):
            self.last_update_time = now
            return True
        return False

    def reset(self) -> None:
        """Reset batch counter (useful when starting a new table)."""
        self.batch_count = 0
        self.last_update_time = time.time()

    def force_update(self) -> None:
        """Force an update on next call to should_update."""
        self.batch_count = self.update_interval
        self.last_update_time = 0

