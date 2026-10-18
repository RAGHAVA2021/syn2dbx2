"""Send one complete, structured run summary to an operator-owned HTTPS endpoint.

The receiver (for example an Azure Logic App) delivers the email and deduplicates
on notification_id. A secret URL is never written to the control log.
"""
import json
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from . import planning


def summary_payload(plan, summary, repair_count, recovery_cutoff, timezone):
    """Use the complete plan, including tables with no terminal log entry."""
    failures = summary["mandatory_failures"] + summary["other_failures"]
    successes = sum(n for status, n in summary["counts"].items() if status in planning.SUCCESS)
    return {
        "notification_id": f"{summary['job_run_id']}:{int(repair_count)}",
        "release_id": summary["release_id"],
        "job_run_id": summary["job_run_id"],
        "business_date": str(plan[0]["business_date"]),
        "result": "SUCCESS" if not failures and summary["ok"] else "NEEDS_ATTENTION",
        "total_tables": len(plan),
        "successful_tables": successes,
        "failed_tables": len(failures),
        "status_counts": summary["counts"],
        "failures": failures,
        "mandatory_failures": len(summary["mandatory_failures"]),
        "recovery_cutoff_local_time": f"{recovery_cutoff} {timezone}",
        "job_ok": bool(summary["ok"]),
    }


def send_summary(dbutils, settings, payload, *, opener=urlopen):
    cfg = settings["notifications"]["summary_webhook"]
    url = dbutils.secrets.get(scope=cfg["secret_scope"], key=cfg["url_secret_key"])
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password or parts.fragment:
        raise ValueError("summary webhook secret must contain an HTTPS URL without userinfo or fragment")
    data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    request = Request(url, data=data, headers={"Content-Type": "application/json",
                                                 "Idempotency-Key": payload["notification_id"]}, method="POST")
    try:
        with opener(request, timeout=15) as response:
            if not 200 <= response.status < 300:
                raise RuntimeError(f"summary notification returned HTTP {response.status}")
    except Exception as exc:
        # HTTPError and URLError may contain the secret URL in their string form.
        raise RuntimeError(f"summary notification delivery failed ({type(exc).__name__}); "
                           "check the configured endpoint and delivery logs") from None
