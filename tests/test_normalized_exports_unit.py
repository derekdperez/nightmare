from __future__ import annotations

import json

from nightmare_app.normalized_exports import write_normalized_exports


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_write_normalized_exports_writes_redirects_and_findings(tmp_path):
    root_domain = "example.com"
    output_root = tmp_path / "output"
    domain_output_dir = output_root / root_domain
    collected_data_root = domain_output_dir / "collected_data"
    endpoints_dir = collected_data_root / "endpoints"
    evidence_dir = domain_output_dir / f"{root_domain}_evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    endpoints_dir.mkdir(parents=True, exist_ok=True)

    _write_json(
        endpoints_dir / "redirect_endpoint.json",
        {
            "captured_at_utc": "2026-04-17T00:00:00Z",
            "source_type": "crawl_response",
            "request": {
                "method": "GET",
                "url": "https://example.com/start",
                "headers": {"Accept": "*/*"},
                "body_base64": "",
            },
            "response": {
                "status": 302,
                "url": "https://example.com/start",
                "headers": {"Location": "http://example.com/login"},
                "body_base64": "",
            },
        },
    )

    url_inventory_payload = {
        "entries": [
            {
                "url": "https://example.com/missing",
                "soft_404_detected": True,
                "soft_404_reason": "known not-found phrase",
                "crawl_status_code": 503,
                "exists_confirmed": False,
                "discovered_via": ["crawl_response"],
            }
        ]
    }
    legacy_requests_payload = {
        "total_requested_urls": 1,
        "entries": [
            {
                "url": "https://example.com/start",
                "found_directly": True,
                "guessed": False,
                "inferred": False,
                "exists": True,
            }
        ],
    }

    redirects_output_path = domain_output_dir / "redirects.json"
    findings_output_path = domain_output_dir / "findings.json"

    write_normalized_exports(
        root_domain=root_domain,
        domain_output_dir=output_root,
        collected_data_root=collected_data_root,
        evidence_dir=evidence_dir,
        url_inventory_payload=url_inventory_payload,
        legacy_requests_payload=legacy_requests_payload,
        config={},
        normalized_data_dir=domain_output_dir / "normalized_data",
        requests_output_path=domain_output_dir / "requests.json",
        sitemap_json_output_path=domain_output_dir / "sitemap.json",
        sitemap_xml_output_path=domain_output_dir / "sitemap.xml",
        sitemap_html_output_path=domain_output_dir / "sitemap.html",
        cookies_output_path=domain_output_dir / "cookies.json",
        scripts_output_path=domain_output_dir / "scripts.json",
        high_value_output_path=domain_output_dir / "high-value.json",
        pages_output_path=domain_output_dir / "pages.json",
        redirects_output_path=redirects_output_path,
        findings_output_path=findings_output_path,
        cookies_dir_output_path=domain_output_dir / "cookies",
        scripts_dir_output_path=domain_output_dir / "scripts",
        high_value_dir_output_path=domain_output_dir / "high-value",
        pages_dir_output_path=domain_output_dir / "pages",
        source_high_value_root=output_root / "high_value" / root_domain,
    )

    redirects_payload = json.loads(redirects_output_path.read_text(encoding="utf-8"))
    assert redirects_payload["redirect_count"] == 1
    assert redirects_payload["redirects"][0]["from_url"] == "https://example.com/start"
    assert redirects_payload["redirects"][0]["to_url"] == "http://example.com/login"

    findings_payload = json.loads(findings_output_path.read_text(encoding="utf-8"))
    finding_types = {entry["type"] for entry in findings_payload["findings"]}
    assert "soft_404" in finding_types
    assert "server_error" in finding_types
    assert "insecure_redirect_target" in finding_types
