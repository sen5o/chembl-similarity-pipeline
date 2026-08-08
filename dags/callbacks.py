"""Failure notification -> MS Teams via a Power Automate webhook (Adaptive Card).

Wired as `on_failure_callback` in the DAG's default_args, so every task gets it
without repeating the hook per task.

The callback must never be the reason a run looks worse than it is: a broken or
unreachable webhook is logged and swallowed, because failing inside a failure
handler would mask the original error that actually matters.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

log = logging.getLogger(__name__)

WEBHOOK_TIMEOUT_SECONDS = 10


def _adaptive_card(dag_id: str, task_id: str, run_id: str, when: str, error: str) -> dict:
    """Power Automate expects an Adaptive Card wrapped in a message attachment."""
    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": "ChEMBL similarity pipeline — task failed",
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": "Attention",
                            "wrap": True,
                        },
                        {
                            "type": "FactSet",
                            "facts": [
                                {"title": "DAG", "value": dag_id},
                                {"title": "Task", "value": task_id},
                                {"title": "Run", "value": run_id},
                                {"title": "Failed at", "value": when},
                            ],
                        },
                        {
                            "type": "TextBlock",
                            "text": error,
                            "wrap": True,
                            "fontType": "Monospace",
                            "spacing": "Medium",
                        },
                    ],
                },
            }
        ],
    }


def notify_teams_on_failure(context) -> None:
    """Airflow on_failure_callback: post an Adaptive Card describing the failure."""
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")

    dag_id = getattr(task_instance, "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = getattr(dag_run, "run_id", "unknown")
    when = str(context.get("ts", "unknown"))

    exception = context.get("exception")
    # Long tracebacks make the card unreadable and can exceed Teams' size limit.
    error = str(exception) if exception else "no exception object in context"
    if len(error) > 800:
        error = error[:800] + " …(truncated)"

    log.error("Task failed: %s.%s (run %s)", dag_id, task_id, run_id)

    webhook = os.environ.get("TEAMS_WEBHOOK_URL")
    if not webhook:
        log.warning("TEAMS_WEBHOOK_URL not set; skipping Teams notification.")
        return

    payload = json.dumps(_adaptive_card(dag_id, task_id, run_id, when, error)).encode()
    request = urllib.request.Request(
        webhook, data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=WEBHOOK_TIMEOUT_SECONDS) as response:
            log.info("Teams notification sent (HTTP %s)", response.status)
    except (urllib.error.URLError, OSError) as exc:
        # Never raise from a failure handler — it would replace the real error.
        log.warning("Could not send Teams notification: %s", exc)
