"""MS Teams notifications for scientists, via a Power Automate Workflows
webhook (the classic Teams "Incoming Webhook" connector was retired by
Microsoft; Workflows + Adaptive Cards is the current supported path).

Framework-agnostic on purpose (no Airflow import) — the DAG wires these
into on_failure_callback. If MS_TEAMS_WEBHOOK_URL isn't set (no webhook has
been issued for this course yet), every notify_* call is a logged no-op:
missing configuration must never break the pipeline.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

log = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 10


def _resolve_webhook_url(webhook_url: str | None) -> str:
    return (webhook_url or os.environ.get("MS_TEAMS_WEBHOOK_URL", "")).strip()


def _post_adaptive_card(webhook_url: str, title: str, text: str, color: str = "Attention") -> None:
    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": title,
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": color,
                        },
                        {"type": "TextBlock", "text": text, "wrap": True},
                    ],
                },
            }
        ],
    }
    data = json.dumps(card).encode("utf-8")
    request = urllib.request.Request(
        webhook_url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
            log.info("Teams notification sent (status %s).", response.status)
    except urllib.error.URLError as exc:
        # A failed notification must never fail the pipeline run itself.
        log.warning("Failed to send Teams notification: %s", exc)


def notify_failure(
    dag_id: str,
    task_id: str,
    run_id: str,
    error: str,
    log_url: str | None = None,
    webhook_url: str | None = None,
) -> None:
    """Posts a failure card to the Teams channel. No-op if no webhook is configured."""
    resolved_url = _resolve_webhook_url(webhook_url)
    if not resolved_url:
        log.info(
            "MS_TEAMS_WEBHOOK_URL not set; skipping Teams failure notification for %s.%s",
            dag_id, task_id,
        )
        return

    text = f"**DAG:** {dag_id}  \n**Task:** {task_id}  \n**Run:** {run_id}  \n**Error:** {error}"
    if log_url:
        text += f"  \n[View logs]({log_url})"
    _post_adaptive_card(resolved_url, "⚠️ Cheminformatics pipeline task failed", text, color="Attention")


def notify_run_summary(
    dag_id: str,
    run_id: str,
    processed_datasets: list[str],
    webhook_url: str | None = None,
) -> None:
    """Posts a weekly run summary card. No-op if no webhook is configured."""
    resolved_url = _resolve_webhook_url(webhook_url)
    if not resolved_url:
        log.info(
            "MS_TEAMS_WEBHOOK_URL not set; skipping Teams run summary for %s", dag_id
        )
        return

    if not processed_datasets:
        text = f"**DAG:** {dag_id}  \n**Run:** {run_id}  \nNo new datasets found this run."
    else:
        listed = ", ".join(processed_datasets)
        text = (
            f"**DAG:** {dag_id}  \n**Run:** {run_id}  \n"
            f"Processed {len(processed_datasets)} dataset(s): {listed}"
        )
    _post_adaptive_card(resolved_url, "✅ Cheminformatics pipeline run complete", text, color="Good")
