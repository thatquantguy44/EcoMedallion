"""Progress tracking for long-running pipeline stages (Gold rebuild, etc.).

Manages a live terminal progress bar + file logging, tracking stage completion
times for ETA calculation. Integrates with the existing logging system via a
logging handler that captures stage completion messages.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import timedelta
from pathlib import Path

try:
    from rich.console import Console
    from rich.progress import BarColumn, Progress, TextColumn, TimeRemainingColumn
except ImportError:
    Progress = None


class ProgressLoggingHandler(logging.Handler):
    """Logging handler that updates a ProgressTracker based on stage completion messages.

    Watches for log records matching "gold.{stage_name} finished in {time}s" and
    calls tracker.stage_done() automatically.
    """

    STAGE_PATTERN = re.compile(r"gold\.(\S+) finished in ([\d.]+)s")

    def __init__(self, tracker: ProgressTracker):
        super().__init__()
        self.tracker = tracker

    def emit(self, record: logging.LogRecord) -> None:
        """Intercept log records and update progress if they're stage completions."""
        try:
            message = record.getMessage()
            match = self.STAGE_PATTERN.search(message)
            if match:
                stage_name, elapsed_str = match.groups()
                elapsed = float(elapsed_str)
                self.tracker.stage_done(stage_name, elapsed)
        except Exception:  # noqa: BLE001, S110
            # Silently ignore: we don't want progress tracking errors to break logging
            pass


class ProgressTracker:
    """Live progress bar + file logging for multi-stage pipeline runs.

    Tracks stage completion times to calculate ETA, displays live progress
    in terminal and logs to both console and file.
    """

    # Location of historical stage timings (for ETA calculation)
    TIMINGS_DB = Path(__file__).parent.parent.parent / ".pipeline_stage_timings.json"

    def __init__(
        self,
        log_file: Path | None = None,
        total_estimated_stages: int = 55,  # Approximate number of Gold tables
    ):
        """Initialize progress tracker.

        Args:
            log_file: Optional file to log progress to (default: no file logging).
            total_estimated_stages: Estimated total number of stages (for display).
                Will adjust dynamically as stages complete.
        """
        self.log_file = log_file
        self.total_stages = total_estimated_stages
        self.stages_seen: list[str] = []
        self.current_stage_idx = 0
        self.start_time = time.time()
        self.stage_timings: dict[str, float] = self._load_timings()

        # Setup file logging if requested
        self.file_handler = None
        if log_file:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            self.file_handler = logging.FileHandler(log_file)
            self.file_handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            )
            logging.getLogger().addHandler(self.file_handler)

        # Attach logging handler to intercept stage completion messages
        self.log_handler = ProgressLoggingHandler(self)
        logging.getLogger().addHandler(self.log_handler)

        # Rich progress display (if available)
        self.progress: Progress | None = None
        self.progress_task = None
        if Progress:
            self.progress = Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(bar_width=30),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeRemainingColumn(),
                console=Console(),
            )
            self.progress.__enter__()
            self.progress_task = self.progress.add_task(
                self._format_task_desc(), total=self.total_stages
            )

        self.logger = logging.getLogger(__name__)

    def stage_done(self, stage_name: str, elapsed: float) -> None:
        """Record a stage completion (called by ProgressLoggingHandler).

        Args:
            stage_name: Name of the completed stage.
            elapsed: Time taken (in seconds).
        """
        self.stages_seen.append(stage_name)
        self.current_stage_idx = len(self.stages_seen)
        self.stage_timings[stage_name] = elapsed

        # Update progress bar
        if self.progress and self.progress_task is not None:
            self.progress.update(self.progress_task, advance=1)
            self.progress.update(
                self.progress_task, description=self._format_task_desc()
            )

        # Periodically save updated timings
        if self.current_stage_idx % 5 == 0:
            self._save_timings()

    def finish(self) -> None:
        """Finalize progress tracking and close log file."""
        elapsed = time.time() - self.start_time
        msg = f"Gold rebuild complete: {elapsed:.1f}s total ({elapsed / 60:.1f} min)"
        self.logger.info(msg)

        if self.progress:
            self.progress.__exit__(None, None, None)

        # Remove handlers
        logging.getLogger().removeHandler(self.log_handler)
        if self.file_handler:
            self.file_handler.close()
            logging.getLogger().removeHandler(self.file_handler)

        # Save final timings
        self._save_timings()

    def _format_task_desc(self) -> str:
        """Format progress bar task description with ETA."""
        if not self.stages_seen:
            return "Starting Gold rebuild..."

        last_stage = self.stages_seen[-1]
        eta = self._calculate_eta()
        eta_str = f" | ETA {eta}" if eta else ""
        pct = (self.current_stage_idx / self.total_stages) * 100
        return f"Stage {self.current_stage_idx}/{self.total_stages} ({pct:.0f}%) | {last_stage}{eta_str}"

    def _calculate_eta(self) -> str | None:
        """Calculate estimated time remaining based on historical timings of remaining stages."""
        # Get average time per stage from known timings
        if not self.stage_timings:
            return None

        avg_stage_time = sum(self.stage_timings.values()) / len(self.stage_timings)
        remaining_stages = max(0, self.total_stages - self.current_stage_idx)
        total_remaining_seconds = remaining_stages * avg_stage_time

        if total_remaining_seconds <= 0:
            return None

        delta = timedelta(seconds=total_remaining_seconds)
        # Format as "Xm Ys" if under 1 hour, else "Xh Ym"
        total_seconds = int(delta.total_seconds())
        if total_seconds < 3600:
            mins, secs = divmod(total_seconds, 60)
            return f"{mins}m {secs}s"
        else:
            hours, remainder = divmod(total_seconds, 3600)
            mins = remainder // 60
            return f"{hours}h {mins}m"

    def _load_timings(self) -> dict[str, float]:
        """Load historical stage timings from disk."""
        if self.TIMINGS_DB.exists():
            try:
                return json.loads(self.TIMINGS_DB.read_text())
            except (OSError, json.JSONDecodeError):
                return {}
        return {}

    def _save_timings(self) -> None:
        """Save current stage timings to disk for future ETA calculation."""
        try:
            self.TIMINGS_DB.write_text(json.dumps(self.stage_timings, indent=2))
        except OSError:
            pass  # Silently fail if we can't write (shouldn't block the build)
