from workflow_app.store import workflow_retry_limit, workflow_total_attempts, _preconditions_require_wait


def test_retry_policy_defaults_to_three_retries_four_total_attempts():
    assert workflow_retry_limit(None) == 3
    assert workflow_total_attempts(None) == 4


def test_retry_policy_zero_means_no_retries_single_attempt():
    assert workflow_retry_limit(0) == 0
    assert workflow_total_attempts(0) == 1


def test_subdomain_enumeration_never_waits_for_prerequisites():
    assert _preconditions_require_wait(
        {"artifacts_all": ["stale_missing_artifact"]},
        plugin_key="recon_subdomain_enumeration",
    ) is False
