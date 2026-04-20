
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.parse import urlparse

from auth0r.canonicalize import canonicalize_url, likely_state_changing
from auth0r.differential_analyzer import compare_responses
from auth0r.lifecycle_analyzer import assess_login_rotation, assess_logout_invalidation, assess_parallel_sessions
from auth0r.login_orchestrator import establish_session
from auth0r.policy import evaluate_action, should_verify_side_effects
from auth0r.profile_store import Auth0rProfileStore
from auth0r.replay_engine import DomainThrottle, replay_variants
from auth0r.reporting import write_summary
from auth0r.side_effect_verifier import verify_side_effect


def _read_json_dict(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}



def _discover_seed_actions(root_domain: str, nightmare_session_path: Path) -> list[dict]:
    payload = _read_json_dict(nightmare_session_path)
    state = payload.get("state", {}) if isinstance(payload, dict) else {}
    out = []
    seen = set()

    def add_action(url: str, method: str = "GET", source: str = "nightmare_seed", metadata: dict | None = None, body=None, content_type: str = "", headers: dict | None = None):
        text = str(url or "").strip()
        if not text:
            return
        parsed = urlparse(text)
        host = (parsed.hostname or "").lower()
        if root_domain not in host:
            return
        canon = canonicalize_url(text)
        dedupe = f"{method.upper()} {canon}"
        if dedupe in seen:
            return
        seen.add(dedupe)
        out.append({
            "url": text,
            "method": method.upper(),
            "source": source,
            "body": body,
            "headers": dict(headers or {}),
            "content_type": content_type or "",
            "metadata": {"canonical_url": canon, **(metadata or {})},
        })

    for url in state.get("discovered_urls", []) or []:
        add_action(url, "GET", "nightmare_discovered_url")

    url_inventory = state.get("url_inventory", {}) if isinstance(state.get("url_inventory", {}), dict) else {}
    for url, record in url_inventory.items():
        if not isinstance(record, dict):
            add_action(url, "GET", "nightmare_inventory")
            continue
        metadata = {
            "discovered_via": list(record.get("discovered_via", []) or []),
            "status_code": record.get("status_code"),
        }
        add_action(url, "GET", "nightmare_inventory", metadata)

    link_graph = state.get("link_graph", {}) if isinstance(state.get("link_graph", {}), dict) else {}
    for src, targets in link_graph.items():
        for target in targets or []:
            add_action(target, "GET", "nightmare_link_graph", {"linked_from": src})

    def walk(node, source: str = "nightmare_structured"):
        if isinstance(node, dict):
            raw_url = node.get("url") or node.get("action") or node.get("endpoint") or node.get("request_url")
            raw_method = node.get("method") or node.get("http_method") or "GET"
            if raw_url:
                metadata = {}
                for key in ("form_name", "field_names", "source_file", "linked_from", "discovered_via"):
                    if key in node:
                        metadata[key] = node.get(key)
                add_action(
                    str(raw_url),
                    str(raw_method).upper(),
                    source,
                    metadata=metadata,
                    body=node.get("body") or node.get("request_body"),
                    content_type=str(node.get("content_type", "") or ""),
                    headers=(node.get("headers") if isinstance(node.get("headers"), dict) else {}),
                )
            for key, value in node.items():
                next_source = source
                if str(key).lower() in {"forms", "requests", "api_calls", "graphql_operations", "actions"}:
                    next_source = f"nightmare_{str(key).lower()}"
                walk(value, next_source)
        elif isinstance(node, list):
            for item in node[:500]:
                walk(item, source)

    walk(state)
    return out


def _marker_hits(text: str, markers) -> list[str]:
    haystack = text or ""
    hits = []
    for marker in markers or []:
        value = getattr(marker, "value", "")
        if value and value in haystack:
            hits.append(value)
    return hits


def run(
    root_domain: str,
    nightmare_session_path: Path,
    summary_path: Path,
    *,
    database_url: str,
    min_delay_seconds: float,
    verify_tls: bool,
    max_seed_actions: int,
    timeout_seconds: float,
) -> int:
    try:
        store = Auth0rProfileStore(database_url)
    except Exception as exc:
        write_summary(summary_path, {"root_domain": root_domain, "status": "failed", "reason": "profile_store_init_failed", "error": str(exc)})
        return 2
    identities = store.list_enabled_identities(root_domain)
    if not identities:
        write_summary(summary_path, {"root_domain": root_domain, "status": "skipped", "reason": "no_enabled_auth_profiles"})
        return 0

    seed_actions = _discover_seed_actions(root_domain, nightmare_session_path)[: max(1, max_seed_actions)]
    throttle = DomainThrottle(min_delay_seconds=min_delay_seconds)
    findings = []
    lifecycle_findings = []
    skipped_actions = []
    session_count = 0
    replay_count = 0

    for idx, identity in enumerate(identities):
        base_url = identity.authenticated_probe_url or (seed_actions[0]["url"] if seed_actions else identity.login_url)
        client, source_type, verification_summary = establish_session(identity, base_url, verify_tls=verify_tls, timeout_seconds=timeout_seconds)
        secondary_client = None
        cross_identity_client = None
        try:
            rotation = assess_login_rotation(verification_summary.get("pre_login_cookies", []), client)
            store.save_finding(root_domain, identity.id, "session_lifecycle", rotation.severity, rotation.title, base_url, "", rotation.confidence, rotation.summary)
            lifecycle_findings.append({"title": rotation.title, "identity_label": identity.identity_label, "summary": rotation.summary})
            session_count += 1
            runtime_session_id = store.save_runtime_session(
                root_domain,
                identity.id,
                source_type,
                1,
                verified=True,
                verification_summary=verification_summary,
                cookies=[{"name": c.name, "value": c.value, "domain": c.domain, "path": c.path} for c in client.cookies.jar],
                auth_headers=dict(client.headers),
            )
            store.save_evidence(root_domain, identity.id, "verification", runtime_session_id, verification_summary)

            try:
                secondary_client, _, secondary_verification = establish_session(identity, base_url, verify_tls=verify_tls, timeout_seconds=timeout_seconds)
                secondary_runtime_id = store.save_runtime_session(
                    root_domain,
                    identity.id,
                    "fresh_login",
                    2,
                    verified=True,
                    verification_summary=secondary_verification,
                    cookies=[{"name": c.name, "value": c.value, "domain": c.domain, "path": c.path} for c in secondary_client.cookies.jar],
                    auth_headers=dict(secondary_client.headers),
                )
                session_count += 1
                overlap = assess_parallel_sessions(client, secondary_client)
                store.save_finding(root_domain, identity.id, "session_lifecycle", overlap.severity, overlap.title, base_url, "second_fresh_session", overlap.confidence, overlap.summary)
                lifecycle_findings.append({"title": overlap.title, "identity_label": identity.identity_label, "summary": overlap.summary})
                store.save_evidence(root_domain, identity.id, "session_parallel_compare", secondary_runtime_id, overlap.summary)
            except Exception as exc:
                secondary_client = None
                store.save_finding(root_domain, identity.id, "session_lifecycle", "low", "Unable to establish second fresh session", base_url, "second_fresh_session", 0.4, {"error": str(exc)})
                lifecycle_findings.append({"title": "Unable to establish second fresh session", "identity_label": identity.identity_label, "error": str(exc)})

            if len(identities) > 1:
                for other in identities:
                    if other.id != identity.id:
                        try:
                            cross_identity_client, _, _ = establish_session(other, other.authenticated_probe_url or base_url, verify_tls=verify_tls, timeout_seconds=timeout_seconds)
                        except Exception:
                            cross_identity_client = None
                        break

            for action in seed_actions:
                action["likely_state_changing"] = likely_state_changing(action.get("method"))
                allowed, reason = evaluate_action(identity.replay_policy, action)
                if not allowed:
                    skipped_actions.append({"identity_label": identity.identity_label, "url": action.get("url"), "method": action.get("method"), "reason": reason})
                    continue
                recorded_action_id = store.save_recorded_action(root_domain, identity.id, runtime_session_id, action)

                throttle.wait()
                baseline = client.request(action.get("method", "GET"), action["url"])
                baseline_auth_hits = _marker_hits(baseline.text, identity.success_markers)

                variants = replay_variants(
                    client,
                    action,
                    throttle=throttle,
                    timeout_seconds=timeout_seconds,
                    verify_tls=verify_tls,
                    success_markers=identity.success_markers,
                    denial_markers=identity.denial_markers,
                    logout_url=identity.logout_url,
                    secondary_client=secondary_client,
                    cross_identity_client=cross_identity_client,
                )
                for variant, candidate, auth_hits, denial_hits in variants:
                    summary = compare_responses(
                        baseline,
                        candidate,
                        authenticated_hits=auth_hits,
                        denial_hits=denial_hits,
                        baseline_authenticated_hits=baseline_auth_hits,
                    )
                    replay_count += 1
                    side_effect = verify_side_effect(action, baseline, candidate, comparison=summary) if should_verify_side_effects(identity.replay_policy, action) else {"checked": False, "reason": "policy_disabled_or_not_state_changing"}
                    summary["side_effect_verification"] = side_effect
                    if side_effect.get("suspicious_side_effect_equivalence"):
                        summary["suspicious"] = True
                    replay_id = store.save_replay_attempt(
                        root_domain,
                        identity.id,
                        recorded_action_id,
                        variant,
                        status_code=(candidate.status_code if candidate is not None else None),
                        suspicious=bool(summary.get("suspicious")),
                        summary=summary,
                    )
                    store.save_evidence(
                        root_domain,
                        identity.id,
                        "replay_response",
                        replay_id,
                        {
                            "variant": variant,
                            "url": action["url"],
                            "status_code": getattr(candidate, "status_code", None),
                            "headers": dict(getattr(candidate, "headers", {}) or {}),
                            "body_preview": ((candidate.text if candidate is not None else "")[:5000]),
                            "summary": summary,
                        },
                    )
                    if summary.get("suspicious") and variant != "original":
                        title = f"Possible authorization bypass via {variant}"
                        if variant == "cross_identity":
                            title = "Possible cross-identity authorization boundary failure"
                        elif side_effect.get("suspicious_side_effect_equivalence"):
                            title = f"Possible state-changing authorization bypass via {variant}"
                        finding_summary = {
                            "identity_label": identity.identity_label,
                            "role_label": identity.role_label,
                            "tenant_label": identity.tenant_label,
                            "expected_behavior": "degraded auth variant should not match authenticated baseline",
                            "observed_behavior": "response remained materially equivalent to authenticated baseline",
                            "comparison": summary,
                            "source_session_type": source_type,
                            "replay_policy": {
                                "read_only_mode": identity.replay_policy.read_only_mode,
                                "verify_state_changes": identity.replay_policy.verify_state_changes,
                            },
                        }
                        store.save_finding(root_domain, identity.id, "authorization", "high", title, action["url"], variant, 0.88, finding_summary)
                        findings.append({"title": title, "endpoint": action["url"], "variant": variant, "identity_label": identity.identity_label, "comparison": summary})

                    if variant == "post_logout_old_session":
                        logout_assessment = assess_logout_invalidation(candidate, denial_hits=denial_hits, success_hits=auth_hits)
                        store.save_finding(
                            root_domain,
                            identity.id,
                            "session_lifecycle",
                            logout_assessment.severity,
                            logout_assessment.title,
                            action["url"],
                            variant,
                            logout_assessment.confidence,
                            logout_assessment.summary,
                        )
                        lifecycle_findings.append({"title": logout_assessment.title, "identity_label": identity.identity_label, "endpoint": action["url"], "summary": logout_assessment.summary})
        finally:
            client.close()
            if secondary_client is not None:
                secondary_client.close()
            if cross_identity_client is not None:
                cross_identity_client.close()

    payload = {
        "root_domain": root_domain,
        "status": "completed",
        "identity_count": len(identities),
        "runtime_session_count": session_count,
        "seed_action_count": len(seed_actions),
        "replay_attempt_count": replay_count,
        "finding_count": len(findings),
        "lifecycle_finding_count": len(lifecycle_findings),
        "findings": findings[:100],
        "lifecycle_findings": lifecycle_findings[:100],
        "minimum_delay_seconds": min_delay_seconds,
        "skipped_action_count": len(skipped_actions),
        "skipped_actions": skipped_actions[:100],
    }
    write_summary(summary_path, payload)
    return 0


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="auth0r authenticated authorization testing stage")
    p.add_argument("root_domain")
    p.add_argument("--nightmare-session", required=True)
    p.add_argument("--summary-json", required=True)
    p.add_argument("--database-url", default=(os.getenv("AUTH0R_DATABASE_URL", "") or os.getenv("DATABASE_URL", "") or os.getenv("COORDINATOR_DATABASE_URL", "")))
    p.add_argument("--min-delay-seconds", type=float, default=0.25)
    p.add_argument("--max-seed-actions", type=int, default=200)
    p.add_argument("--timeout-seconds", type=float, default=20.0)
    p.add_argument("--insecure-tls", action="store_true")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    return run(
        str(args.root_domain).strip().lower(),
        Path(args.nightmare_session),
        Path(args.summary_json),
        database_url=args.database_url,
        min_delay_seconds=max(0.25, float(args.min_delay_seconds or 0.25)),
        verify_tls=not bool(args.insecure_tls),
        max_seed_actions=max(1, int(args.max_seed_actions or 200)),
        timeout_seconds=max(1.0, float(args.timeout_seconds or 20.0)),
    )


if __name__ == "__main__":
    raise SystemExit(main())
