from nightmare_app import spider_url_policy
from fozzy_app.fuzz_core import (
    ParameterMeta,
    RouteGroup,
    baseline_seed_value_for,
    build_fuzz_http_request,
    merge_data_type,
)
from nightmare_shared.value_types import infer_observed_value_type


def test_infer_observed_value_type_shared():
    assert infer_observed_value_type("123") == "int"
    assert infer_observed_value_type("true") == "bool"
    assert infer_observed_value_type("2026-04-17") == "date"


def test_spider_url_policy_filters_and_condenses():
    urls = [
        "https://example.com/app.abcdef123456.js",
        "https://example.com/app.1234567890ab.js",
        "https://example.com/page",
        "https://example.com/image.png",
    ]
    condensed = spider_url_policy.condense_urls_for_ai(urls, ai_excluded_url_extensions={".png"})
    assert "https://example.com/page" in condensed
    assert all(not item.endswith(".png") for item in condensed)
    assert any("{hashed_bundle}.js" in item for item in condensed)


def test_spider_url_policy_should_crawl_high_value_suffix_even_static():
    url = "https://example.com/private/admin.php"
    should = spider_url_policy.should_crawl_url(
        url,
        static_asset_extensions={".php"},
        high_value_extensions=set(),
        high_value_path_suffixes=("/admin.php",),
    )
    assert should is True


def test_fuzz_core_merge_data_type_and_baseline_seed():
    assert merge_data_type("int", "float", type_priority=["float", "int"]) == "float"
    meta = ParameterMeta(name="id", data_type="int", observed_values={"42", "7"})
    assert baseline_seed_value_for(meta, {"int": "0"}) == "7"


def test_fuzz_core_build_fuzz_http_request_post_query_and_body():
    group = RouteGroup(host="example.com", path="/submit", scheme="https", http_method="POST")
    group.params["q"] = ParameterMeta(name="q", data_type="string")
    group.body_params["token"] = ParameterMeta(name="token", data_type="token")
    group.extra_request_headers["X-Extra"] = "yes"
    method, url, headers, body = build_fuzz_http_request(
        group,
        values={"q": "abc", "token": "secret"},
        base_headers={"Accept": "application/json"},
    )
    assert method == "POST"
    assert url.startswith("https://example.com/submit?")
    assert "q=abc" in url
    assert headers["Accept"] == "application/json"
    assert headers["X-Extra"] == "yes"
    assert headers["Content-Type"] == "application/x-www-form-urlencoded"
    assert body == b"token=secret"

