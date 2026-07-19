"""Failure notification callback -> MS Teams via Power Automate (Adaptive Card).

Iteration 1: stub. Logs the failure and reads the webhook location; the actual
Adaptive Card POST is implemented in the operations iteration.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


def notify_teams_on_failure(context) -> None:
    """Airflow on_failure_callback. `context` carries task/dag/exception info."""
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    log.error("Task failed: %s.%s", dag_id, task_id)

    webhook = os.environ.get("TEAMS_WEBHOOK_URL")
    if not webhook:
        log.warning("TEAMS_WEBHOOK_URL not set; skipping notification.")
        return

    # TODO(feature/operations): build and POST an Adaptive Card to `webhook`.
