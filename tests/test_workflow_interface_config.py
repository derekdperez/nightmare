import json
from pathlib import Path

from server import _workflow_interface_catalog_payload


def test_run_recon_workflow_declares_control_and_results_interfaces() -> None:
    payload = json.loads(Path("workflows/run-recon.workflow.json").read_text(encoding="utf-8"))
    interfaces = payload.get("interfaces")
    assert isinstance(interfaces, dict)
    control = interfaces.get("control")
    results = interfaces.get("results")
    assert isinstance(control, dict)
    assert isinstance(results, dict)
    assert control.get("route") == "/workflow-interfaces/run-recon/control"
    assert results.get("route") == "/workflow-interfaces/run-recon/results"
    assert str(control.get("template") or "").endswith("recon_control.html.j2")
    assert str(results.get("template") or "").endswith("recon_results.html.j2")


def test_workflow_interface_catalog_discovers_recon_interfaces() -> None:
    catalog = _workflow_interface_catalog_payload()
    interfaces = catalog.get("interfaces")
    assert isinstance(interfaces, list)
    routes = {str(item.get("route") or "") for item in interfaces if isinstance(item, dict)}
    assert "/workflow-interfaces/run-recon/control" in routes
    assert "/workflow-interfaces/run-recon/results" in routes


def test_workflow_interface_catalog_routes_are_unique() -> None:
    catalog = _workflow_interface_catalog_payload()
    interfaces = catalog.get("interfaces")
    assert isinstance(interfaces, list)
    route_list = [str(item.get("route") or "") for item in interfaces if isinstance(item, dict) and str(item.get("route") or "")]
    assert len(route_list) == len(set(route_list))


def test_navbar_requests_dynamic_workflow_interface_links() -> None:
    template = Path("templates/_navbar.html.j2").read_text(encoding="utf-8")
    assert 'id="workflowInterfaceNav"' in template
    assert "/api/coord/workflow-interfaces" in template


def test_navbar_primary_links_match_reduced_page_set() -> None:
    template = Path("templates/_navbar.html.j2").read_text(encoding="utf-8")
    assert 'href="/workers"' in template
    assert 'href="/dashboard"' in template
    assert 'href="/workflows"' in template
    assert 'href="/crawl-progress"' in template
    assert 'href="/http-requests"' in template
    assert 'href="/extractor-matches"' in template
    assert 'href="/fuzzing"' in template
    assert 'href="/workflow-definitions"' in template
    assert 'href="/database"' in template
    assert 'href="/docker-status"' in template
    assert 'href="/view-logs"' in template

    assert 'href="/operations"' not in template
    assert 'href="/plugin-definitions"' not in template
    assert 'href="/workflow-runs"' not in template
    assert 'href="/events"' not in template
    assert 'href="/auth0r"' not in template
    assert 'href="/errors"' not in template
    assert 'href="/discovered-targets"' not in template
    assert 'href="/discovered-files"' not in template


def test_workflows_page_removes_legacy_panels_and_uses_task_actions() -> None:
    template = Path("templates/workflows.html.j2").read_text(encoding="utf-8")
    assert "Enqueue Plugin Task" not in template
    assert "Reset Plugin Tasks" not in template
    assert ">Monitor<" not in template
    assert ">Timeline<" not in template
    assert 'data-action="delete"' in template
    assert 'data-action="pause"' in template
    assert 'data-action="run"' in template
