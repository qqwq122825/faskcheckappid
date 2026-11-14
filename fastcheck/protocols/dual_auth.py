"""Optional GSA -> web recheck for init-stage -20970 responses.

The first GSA request does not contain the SRP password proof.  When Apple
returns -20970 there, an enabled dual-protocol task runs one IDMSA web exchange
on the same account and proxy.  Only the web result becomes the public verdict;
the GSA response is retained as bounded metadata for diagnostics.
"""
from __future__ import annotations

from . import idmsa


def needs_web_recheck(result: dict) -> bool:
    return result.get("stage") == "init" and result.get("apple_ec") == -20970


def _web_metadata(event: dict, target: dict) -> None:
    stage = event.get("stage")
    if stage in ("init", "complete"):
        target[f"{stage}_http"] = event.get("http")
        target["http"] = event.get("http")
    codes = event.get("service_error_codes") or []
    if codes:
        try:
            target["apple_ec"] = int(codes[0])
        except (TypeError, ValueError):
            pass


def merge_results(primary: dict, web: dict, metadata: dict | None = None) -> dict:
    """Use the web verdict while retaining the reason the fallback ran."""
    result = dict(web)
    result.update(metadata or {})
    result.update(
        fallback_used=True,
        verification_protocol="web",
        gsa_status=primary.get("status", "error"),
        gsa_stage=primary.get("stage", ""),
        gsa_http=primary.get("http"),
        gsa_apple_ec=primary.get("apple_ec"),
        web_status=web.get("status", "error"),
        web_stage=web.get("stage", ""),
        web_kind=web.get("kind", ""),
    )
    if "http" in result:
        result["web_http"] = result["http"]
    if "apple_ec" in result:
        result["web_apple_ec"] = result["apple_ec"]
    if result.get("status") in ("valid", "need_2fa", "needs_setup"):
        # These IDMSA verdicts are emitted only after the complete SRP request.
        result["password_verified"] = True
    result["ms"] = int(primary.get("ms") or 0) + int(web.get("ms") or 0)
    return result


def recheck_gsa_20970(email: str, password: str, proxy: str | None, primary: dict,
                      *, enabled: bool, timeout: float, cancel=None,
                      diagnostics=None, session=None, checker=None,
                      retries: int = 0) -> dict:
    """Run one web recheck when enabled; HTTP 5xx remains a retryable error."""
    if not enabled or not needs_web_recheck(primary):
        return primary
    trace = {}

    def remember(event):
        _web_metadata(event, trace)

    check = checker or idmsa.check
    web = check(email, password, proxy, timeout=timeout, retries=max(0, int(retries)),
                cancel=cancel, trace=remember, diagnostics=diagnostics,
                session=session)
    return merge_results(primary, web, trace)
