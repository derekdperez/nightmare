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


def test_navbar_requests_dynamic_workflow_interface_links() -> None:
    template = Path("templates/_navbar.html.j2").read_text(encoding="utf-8")
    assert 'id="workflowInterfaceNav"' in template
    assert "/api/coord/workflow-interfaces" in template
