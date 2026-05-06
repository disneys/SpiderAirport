import json
import logging
import os
import re
from copy import deepcopy
from urllib.parse import urljoin

import requests

from airport import (
    DEFAULT_OVERSEER_TEMPLATE,
    MAIN_JS_URL,
    SUBSCRIPTION_HEADERS,
    add_generation_marker_proxy_group,
    apply_remote_main_js_with_loop_resolution,
    build_clash_file_text,
    build_fallback_config_with_loop_resolution,
    build_generation_metadata,
    decode_base64_text,
    deduplicate_proxies,
    ensure_unique_proxy_names,
    extract_yaml_proxies,
    is_probably_base64_payload,
    normalize_text,
    write_text_file,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(ROOT_DIR, "bsbb.txt")
SOURCE_NAME = "bsbb.cc"
BSBB_HOME_URL = "https://www.bsbb.cc/clash/"
BSBB_TOKEN_URL = urljoin(BSBB_HOME_URL, "gentoken.php")
BSBB_DAILY_SUB_PATTERN = re.compile(r"/daily_sub\.php\?token=[a-f0-9]+", re.I)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def fetch_bsbb_subscription_url(session):
    logger.info("Fetching bsbb homepage: %s", BSBB_HOME_URL)
    try:
        homepage_response = session.get(BSBB_HOME_URL, headers=BROWSER_HEADERS, timeout=30)
        if homepage_response.status_code >= 400:
            logger.warning(
                "bsbb homepage returned HTTP %s, continue with token endpoint",
                homepage_response.status_code,
            )
    except requests.RequestException as exc:
        logger.warning("bsbb homepage fetch failed, continue with token endpoint: %s", exc)

    logger.info("Requesting today's bsbb subscription token: %s", BSBB_TOKEN_URL)
    token_headers = dict(BROWSER_HEADERS)
    token_headers["Accept"] = "application/json,text/plain,*/*"
    token_headers["Referer"] = BSBB_HOME_URL
    token_response = session.get(BSBB_TOKEN_URL, headers=token_headers, timeout=30)
    token_response.raise_for_status()

    try:
        payload = token_response.json()
    except json.JSONDecodeError as exc:
        raise ValueError(f"bsbb token response is not JSON: {token_response.text}") from exc

    subscription_url = payload.get("url") if isinstance(payload, dict) else ""
    if not subscription_url:
        raise ValueError(f"bsbb token response missing url: {payload}")

    subscription_url = urljoin(BSBB_HOME_URL, subscription_url)
    if not BSBB_DAILY_SUB_PATTERN.search(subscription_url):
        logger.warning("Unexpected bsbb subscription URL format: %s", subscription_url)
    return subscription_url


def fetch_subscription_content(session, subscription_url):
    headers = dict(SUBSCRIPTION_HEADERS)
    headers["Referer"] = BSBB_HOME_URL
    logger.info("Fetching bsbb subscription content: %s", subscription_url)
    response = session.get(subscription_url, headers=headers, timeout=30)
    response.raise_for_status()
    for encoding in ("utf-8", "utf-8-sig", response.apparent_encoding):
        if not encoding:
            continue
        try:
            return normalize_text(response.content.decode(encoding))
        except UnicodeDecodeError:
            continue
    return normalize_text(response.text)


def decode_subscription_content(raw_content):
    if is_probably_base64_payload(raw_content):
        decoded_content, encoding = decode_base64_text(raw_content)
        logger.info("Decoded bsbb subscription as base64: encoding=%s", encoding)
        return decoded_content
    return raw_content


def read_existing_subscription_url():
    if not os.path.exists(OUTPUT_FILE):
        return ""

    with open(OUTPUT_FILE, "r", encoding="utf-8") as file:
        for line in file:
            if not line.startswith("# "):
                break
            if line.startswith("# subscription_url:"):
                return line.split(":", 1)[1].strip()
    return ""


def build_bsbb_file_text(
    config,
    result,
    rules_source,
    rules_mode,
    generated_at,
    generation_marker_name,
):
    output_text = build_clash_file_text(
        config,
        [result],
        rules_source,
        rules_mode,
        generated_at,
        generation_marker_name,
    )
    subscription_header = f"# subscription_url: {result['subscription_url']}"
    return output_text.replace("\n# rules_source:", f"\n{subscription_header}\n# rules_source:", 1)


def build_bsbb_result(subscription_url, proxies):
    return {
        "source_name": SOURCE_NAME,
        "subscription_url": subscription_url,
        "proxies": proxies,
    }


def build_bsbb_config(proxies):
    unique_proxies = deduplicate_proxies(proxies)
    logger.info("Proxy collection complete: raw=%s, unique=%s", len(proxies), len(unique_proxies))

    named_proxies, duplicate_name_count = ensure_unique_proxy_names(unique_proxies)
    if duplicate_name_count:
        logger.info("Renamed duplicated proxy names: %s", duplicate_name_count)

    rules_source = MAIN_JS_URL
    rules_mode = "remote-main-js-executed"
    try:
        config, _final_proxies, rules_source, rules_mode, loop_renamed_count = (
            apply_remote_main_js_with_loop_resolution(named_proxies)
        )
        if loop_renamed_count:
            logger.info(
                "Renamed %s proxies to resolve proxy-group self references from remote main.js",
                loop_renamed_count,
            )
    except Exception as exc:
        logger.error("Remote main.js apply failed, fallback to embedded template: %s", exc)
        config, _final_proxies, fallback_loop_renamed_count = (
            build_fallback_config_with_loop_resolution(
                named_proxies,
                deepcopy(DEFAULT_OVERSEER_TEMPLATE),
            )
        )
        if fallback_loop_renamed_count:
            logger.info(
                "Renamed %s proxies to resolve proxy-group self references from fallback template",
                fallback_loop_renamed_count,
            )
        rules_mode = "fallback-default"

    generated_at, generation_marker_name = build_generation_metadata()
    config = add_generation_marker_proxy_group(config, generation_marker_name)
    return config, rules_source, rules_mode, generated_at, generation_marker_name


def generate_bsbb_file():
    existing_subscription_url = read_existing_subscription_url()
    with requests.Session() as session:
        try:
            subscription_url = fetch_bsbb_subscription_url(session)
        except (requests.RequestException, ValueError) as exc:
            if existing_subscription_url and os.path.exists(OUTPUT_FILE):
                logger.warning(
                    "Unable to refresh bsbb subscription URL, keep existing file: %s",
                    exc,
                )
                return
            raise

        if existing_subscription_url == subscription_url:
            logger.info(
                "bsbb subscription URL unchanged, skip parsing: %s",
                subscription_url,
            )
            return

        raw_content = fetch_subscription_content(session, subscription_url)

    subscription_content = decode_subscription_content(raw_content)
    proxies = extract_yaml_proxies(subscription_content, SOURCE_NAME)
    result = build_bsbb_result(subscription_url, proxies)
    config, rules_source, rules_mode, generated_at, generation_marker_name = build_bsbb_config(
        result["proxies"]
    )

    output_text = build_bsbb_file_text(
        config,
        result,
        rules_source,
        rules_mode,
        generated_at,
        generation_marker_name,
    )
    write_text_file(OUTPUT_FILE, output_text)
    logger.info("Wrote bsbb clash config: %s", OUTPUT_FILE)


def main():
    generate_bsbb_file()


if __name__ == "__main__":
    main()
