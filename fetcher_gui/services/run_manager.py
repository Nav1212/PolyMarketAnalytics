"""
Run Manager Service

Manages fetcher run operations in background threads.
Provides status tracking and stop capability.
"""

import threading
import time
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable
from enum import Enum
from dataclasses import dataclass, field


class RunMode(str, Enum):
    """Available run modes."""
    ALL = "all"
    MARKETS = "markets"
    TRADES = "trades"
    TRADES_LEADERBOARD = "trades_leaderboard"
    PRICES = "prices"
    LEADERBOARD = "leaderboard"
    GAMMA = "gamma"


class RunStatus(str, Enum):
    """Run status values."""
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class RunProgress:
    """Tracks progress of a run."""
    status: RunStatus = RunStatus.IDLE
    mode: Optional[RunMode] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    items_processed: int = 0
    items_total: int = 0
    current_operation: str = ""
    error_message: str = ""
    log_lines: List[str] = field(default_factory=list)


class RunManager:
    """
    Manages fetcher run operations.

    Runs the fetcher in a subprocess and tracks progress.
    """

    def __init__(self):
        """Initialize the run manager."""
        self._lock = threading.Lock()
        self._progress = RunProgress()
        self._process: Optional[subprocess.Popen] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_requested = False

        # Paths
        self._project_root = Path(__file__).parent.parent.parent
        self._fetcher_main = self._project_root / "fetcher" / "main.py"

    @property
    def is_running(self) -> bool:
        """Check if a run is currently in progress."""
        with self._lock:
            return self._progress.status in (RunStatus.STARTING, RunStatus.RUNNING)

    @property
    def current_mode(self) -> Optional[str]:
        """Get the current run mode."""
        with self._lock:
            return self._progress.mode.value if self._progress.mode else None

    @property
    def progress(self) -> RunProgress:
        """Get the current progress."""
        with self._lock:
            return RunProgress(
                status=self._progress.status,
                mode=self._progress.mode,
                start_time=self._progress.start_time,
                end_time=self._progress.end_time,
                items_processed=self._progress.items_processed,
                items_total=self._progress.items_total,
                current_operation=self._progress.current_operation,
                error_message=self._progress.error_message,
                log_lines=list(self._progress.log_lines[-100:])  # Last 100 lines
            )

    def start(
        self,
        mode: RunMode,
        fresh: bool = False,
        limit: Optional[int] = None,
        timeout: Optional[int] = None,
        on_complete: Optional[Callable] = None
    ) -> bool:
        """
        Start a fetcher run.

        Args:
            mode: The run mode
            fresh: Whether to clear cursors and start fresh
            limit: Optional limit on number of items
            timeout: Optional timeout in seconds
            on_complete: Optional callback when run completes

        Returns:
            True if started successfully, False if already running
        """
        with self._lock:
            if self._progress.status in (RunStatus.STARTING, RunStatus.RUNNING):
                return False

            self._progress = RunProgress(
                status=RunStatus.STARTING,
                mode=mode,
                start_time=datetime.now(),
                current_operation="Initializing..."
            )
            self._stop_requested = False

        # Build command
        cmd = [sys.executable, str(self._fetcher_main), "--mode", mode.value]

        if fresh:
            cmd.append("--fresh")

        if limit is not None:
            cmd.extend(["--limit", str(limit)])

        if timeout is not None:
            cmd.extend(["--timeout", str(timeout)])

        # Start monitor thread
        self._monitor_thread = threading.Thread(
            target=self._run_and_monitor,
            args=(cmd, on_complete),
            daemon=True
        )
        self._monitor_thread.start()

        return True

    def _run_and_monitor(self, cmd: List[str], on_complete: Optional[Callable] = None):
        """
        Run the fetcher process and monitor its output.

        Args:
            cmd: Command to run
            on_complete: Callback when complete
        """
        try:
            # Start process
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(self._project_root)
            )

            with self._lock:
                self._progress.status = RunStatus.RUNNING

            # Monitor output
            for line in iter(self._process.stdout.readline, ''):
                if self._stop_requested:
                    break

                line = line.strip()
                if line:
                    with self._lock:
                        self._progress.log_lines.append(line)

                        # Parse progress from output
                        self._parse_progress_line(line)

            # Wait for process to complete
            self._process.wait()

            with self._lock:
                if self._stop_requested:
                    self._progress.status = RunStatus.CANCELLED
                elif self._process.returncode == 0:
                    self._progress.status = RunStatus.COMPLETED
                else:
                    self._progress.status = RunStatus.FAILED
                    self._progress.error_message = f"Process exited with code {self._process.returncode}"

                self._progress.end_time = datetime.now()

        except Exception as e:
            with self._lock:
                self._progress.status = RunStatus.FAILED
                self._progress.error_message = str(e)
                self._progress.end_time = datetime.now()

        finally:
            self._process = None

            if on_complete:
                try:
                    on_complete(self._progress)
                except Exception:
                    pass

    def _parse_progress_line(self, line: str):
        """
        Parse a log line to extract progress information.

        Args:
            line: Log line to parse
        """
        # Look for common progress patterns
        if "Starting" in line:
            self._progress.current_operation = line

        elif "Fetched" in line:
            self._progress.current_operation = line
            # Try to extract count
            import re
            match = re.search(r'Fetched\s+(\d+)', line)
            if match:
                self._progress.items_processed = int(match.group(1))

        elif "Worker" in line:
            self._progress.current_operation = line

        elif "completed" in line.lower():
            self._progress.current_operation = line

        elif "markets pending" in line.lower():
            import re
            match = re.search(r'(\d+)\s+markets pending', line)
            if match:
                self._progress.items_total = int(match.group(1))

    def stop(self) -> bool:
        """
        Stop the current run.

        Returns:
            True if stop was requested, False if nothing running
        """
        with self._lock:
            if self._progress.status not in (RunStatus.STARTING, RunStatus.RUNNING):
                return False

            self._stop_requested = True
            self._progress.status = RunStatus.STOPPING
            self._progress.current_operation = "Stopping..."

        # Try to terminate the process
        if self._process:
            try:
                self._process.terminate()
                # Give it a few seconds to clean up
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
            except Exception:
                pass

        return True

    def get_log_lines(self, last_n: int = 50) -> List[str]:
        """
        Get recent log lines.

        Args:
            last_n: Number of recent lines to return

        Returns:
            List of log lines
        """
        with self._lock:
            return list(self._progress.log_lines[-last_n:])

    def clear_logs(self):
        """Clear the log buffer."""
        with self._lock:
            self._progress.log_lines = []

    def get_status_dict(self) -> Dict[str, Any]:
        """
        Get status as a dictionary.

        Returns:
            Dict with status information
        """
        with self._lock:
            return {
                "status": self._progress.status.value,
                "mode": self._progress.mode.value if self._progress.mode else None,
                "start_time": self._progress.start_time.isoformat() if self._progress.start_time else None,
                "end_time": self._progress.end_time.isoformat() if self._progress.end_time else None,
                "items_processed": self._progress.items_processed,
                "items_total": self._progress.items_total,
                "current_operation": self._progress.current_operation,
                "error_message": self._progress.error_message
            }
