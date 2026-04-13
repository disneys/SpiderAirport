import argparse
import base64
import binascii
import json
import logging
import os
import re
import socket
import shutil
import subprocess
import tempfile
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit

import requests
import yaml


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(ROOT_DIR, "spider_clash.txt")
ERROR_FILE = os.path.join(ROOT_DIR, "spider_clash_error.txt")
SUBSCRIPTION_HEADERS = {"User-Agent": "clash-verge/v2.4.7"}
MAIN_JS_URL = (
    "https://raw.githubusercontent.com/disneys/"
    "Mihomo-Dynamic-Overseer/refs/heads/main/main.js"
)
UTC_PLUS_8 = timezone(timedelta(hours=8))
EMBEDDED_SOURCE_LABEL = "embedded_sources"
REACHABILITY_TIMEOUT_SECONDS = 1.0
REACHABILITY_MAX_WORKERS = 64
UDP_ONLY_PROXY_TYPES = {"hysteria", "hysteria2", "hy2", "tuic", "wireguard"}
EMBEDDED_SOURCES = [
    {
        "source_name": "clashnodev2ray.github.io",
        "reference_urls": [
            "https://sfdr.zaixianyouxi.dpdns.org/uploads/2026/4/20260413.yaml",
        ],
    },
    {
        "source_name": "clashmetagithub.github.io",
        "reference_urls": [
            "https://clashmetagithub.github.io/uploads/2026/04/0-20260413.yaml",
            "https://clashmetagithub.github.io/uploads/2026/04/1-20260413.yaml",
            "https://clashmetagithub.github.io/uploads/2026/04/2-20260413.yaml",
            "https://clashmetagithub.github.io/uploads/2026/04/3-20260413.yaml",
            "https://clashmetagithub.github.io/uploads/2026/04/4-20260413.yaml",
        ],
    },
    {
        "source_name": "freev2rayclash.github.io",
        "reference_urls": [
            "https://freev2rayclash.github.io/uploads/2026/04/0-20260413.yaml",
            "https://freev2rayclash.github.io/uploads/2026/04/1-20260413.yaml",
            "https://freev2rayclash.github.io/uploads/2026/04/2-20260413.yaml",
            "https://freev2rayclash.github.io/uploads/2026/04/3-20260413.yaml",
            "https://freev2rayclash.github.io/uploads/2026/04/4-20260413.yaml",
        ],
    },
    {
        "source_name": "stashgithub.github.io",
        "reference_urls": [
            "https://stashgithub.github.io/uploads/2026/04/0-20260413.yaml",
            "https://stashgithub.github.io/uploads/2026/04/1-20260413.yaml",
            "https://stashgithub.github.io/uploads/2026/04/2-20260413.yaml",
            "https://stashgithub.github.io/uploads/2026/04/3-20260413.yaml",
            "https://stashgithub.github.io/uploads/2026/04/4-20260413.yaml",
        ],
    },
    {
        "source_name": "windowsclashnode.github.io",
        "reference_urls": [
            "https://windowsclashnode.github.io/uploads/2026/04/0-20260413.yaml",
            "https://windowsclashnode.github.io/uploads/2026/04/1-20260413.yaml",
            "https://windowsclashnode.github.io/uploads/2026/04/2-20260413.yaml",
            "https://windowsclashnode.github.io/uploads/2026/04/3-20260413.yaml",
            "https://windowsclashnode.github.io/uploads/2026/04/4-20260413.yaml",
        ],
    },
    {
        "source_name": "clashnode.github.io",
        "reference_urls": [
            "https://clashnode.github.io/uploads/2026/04/0-20260413.yaml",
            "https://clashnode.github.io/uploads/2026/04/1-20260413.yaml",
            "https://clashnode.github.io/uploads/2026/04/2-20260413.yaml",
            "https://clashnode.github.io/uploads/2026/04/3-20260413.yaml",
            "https://clashnode.github.io/uploads/2026/04/4-20260413.yaml",
        ],
    },
    {
        "source_name": "clashvergerev.github.io",
        "reference_urls": [
            "https://clashvergerev.github.io/uploads/2026/04/0-20260413.yaml",
            "https://clashvergerev.github.io/uploads/2026/04/1-20260413.yaml",
            "https://clashvergerev.github.io/uploads/2026/04/2-20260413.yaml",
            "https://clashvergerev.github.io/uploads/2026/04/3-20260413.yaml",
            "https://clashvergerev.github.io/uploads/2026/04/4-20260413.yaml",
        ],
    },
    {
        "source_name": "clash-verge.github.io",
        "reference_urls": [
            "https://clash-verge.github.io/uploads/2026/04/0-20260413.yaml",
            "https://clash-verge.github.io/uploads/2026/04/1-20260413.yaml",
            "https://clash-verge.github.io/uploads/2026/04/2-20260413.yaml",
            "https://clash-verge.github.io/uploads/2026/04/3-20260413.yaml",
            "https://clash-verge.github.io/uploads/2026/04/4-20260413.yaml",
        ],
    },
    {
        "source_name": "chengaopan/AutoMergePublicNodes",
        "reference_urls": [
            "https://raw.githubusercontent.com/chengaopan/AutoMergePublicNodes/master/list.meta.yml",
        ],
    },
    {
        "source_name": "peasoft/NoMoreWalls",
        "reference_urls": [
            "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.meta.yml",
        ],
    },
]

FALLBACK_OVERSEER_TEMPLATE = {
    "test_url": "http://www.gstatic.com/generate_204",
    "test_interval": 60,
    "rule_base": "https://fastly.jsdelivr.net/gh/Loyalsoldier/clash-rules@release/",
    "icon_base": "https://raw.githubusercontent.com/Koolson/Qure/master/IconSet/Color/",
    "icons": {
        "GLOBAL": "Global.png",
        "AUTO": "Auto.png",
        "MANUAL": "Static.png",
        "AI": "ChatGPT.png",
        "HK": "Hong_Kong.png",
        "TW": "Taiwan.png",
        "SG": "Singapore.png",
        "JP": "Japan.png",
        "US": "United_States.png",
        "KR": "Korea.png",
        "UK": "United_Kingdom.png",
        "DE": "Germany.png",
        "FR": "France.png",
        "CA": "Canada.png",
        "AU": "Australia.png",
        "RU": "Russia.png",
        "IN": "India.png",
        "NL": "Netherlands.png",
        "TR": "Turkey.png",
        "BR": "Brazil.png",
        "OTHER": "Airport.png",
    },
    "rule_providers": [
        {
            "name": "reject",
            "type": "http",
            "behavior": "domain",
            "filename": "reject.txt",
            "path": "./ruleset/reject.yaml",
            "interval": 86400,
        },
        {
            "name": "icloud",
            "type": "http",
            "behavior": "domain",
            "filename": "icloud.txt",
            "path": "./ruleset/icloud.yaml",
            "interval": 86400,
        },
        {
            "name": "apple",
            "type": "http",
            "behavior": "domain",
            "filename": "apple.txt",
            "path": "./ruleset/apple.yaml",
            "interval": 86400,
        },
        {
            "name": "google",
            "type": "http",
            "behavior": "domain",
            "filename": "google.txt",
            "path": "./ruleset/google.yaml",
            "interval": 86400,
        },
        {
            "name": "proxy",
            "type": "http",
            "behavior": "domain",
            "filename": "proxy.txt",
            "path": "./ruleset/proxy.yaml",
            "interval": 86400,
        },
        {
            "name": "direct",
            "type": "http",
            "behavior": "domain",
            "filename": "direct.txt",
            "path": "./ruleset/direct.yaml",
            "interval": 86400,
        },
        {
            "name": "private",
            "type": "http",
            "behavior": "domain",
            "filename": "private.txt",
            "path": "./ruleset/private.yaml",
            "interval": 86400,
        },
        {
            "name": "cncidr",
            "type": "http",
            "behavior": "ipcidr",
            "filename": "cncidr.txt",
            "path": "./ruleset/cncidr.yaml",
            "interval": 86400,
        },
        {
            "name": "lancidr",
            "type": "http",
            "behavior": "ipcidr",
            "filename": "lancidr.txt",
            "path": "./ruleset/lancidr.yaml",
            "interval": 86400,
        },
        {
            "name": "telegramcidr",
            "type": "http",
            "behavior": "ipcidr",
            "filename": "telegramcidr.txt",
            "path": "./ruleset/telegramcidr.yaml",
            "interval": 86400,
        },
        {
            "name": "applications",
            "type": "http",
            "behavior": "classical",
            "filename": "applications.txt",
            "path": "./ruleset/applications.yaml",
            "interval": 86400,
        },
    ],
    "dns": {
        "enable": True,
        "device-network": True,
        "ipv6": False,
        "enhanced-mode": "fake-ip",
        "fake-ip-range": "198.18.0.1/16",
        "default-nameserver": ["223.5.5.5", "119.29.29.29", "1.1.1.1"],
        "nameserver": [
            "223.5.5.5",
            "223.6.6.6",
            "119.29.29.29",
            "180.76.76.76",
            "tls://dns.alidns.com",
            "https://dns.alidns.com/dns-query",
            "https://doh.pub/dns-query",
        ],
        "fallback": [
            "https://dns.google/dns-query",
            "https://cloudflare-dns.com/dns-query",
            "https://dns.quad9.net/dns-query",
            "tls://8.8.8.8",
            "tls://1.1.1.1",
            "tls://dns.quad9.net",
        ],
        "fallback-filter": {
            "geoip": True,
            "geoip-code": "CN",
            "ipcidr": ["240.0.0.0/4"],
        },
        "nameserver-policy": {
            "geosite:cn": "https://dns.alidns.com/dns-query",
            "domain:sub.datapipe.top,suc-store.usuc.cc,sub-aylz.koyeb.app": "119.29.29.29",
        },
    },
    "region_configs": [
        {"name": "香港", "regex": r"香港|HK|Hong Kong|🇭🇰", "icon_key": "HK"},
        {"name": "台湾", "regex": r"台湾|TW|Taiwan|🇹🇼", "icon_key": "TW"},
        {"name": "新加坡", "regex": r"新加坡|狮城|SG|Singapore|🇸🇬", "icon_key": "SG"},
        {"name": "日本", "regex": r"日本|JP|Japan|🇯🇵", "icon_key": "JP"},
        {"name": "美国", "regex": r"美国|US|United States|🇺🇸", "icon_key": "US"},
        {"name": "韩国", "regex": r"韩国|KR|Korea|🇰🇷", "icon_key": "KR"},
        {"name": "英国", "regex": r"英国|UK|United Kingdom|🇬🇧", "icon_key": "UK"},
        {"name": "德国", "regex": r"德国|DE|Germany|🇩🇪", "icon_key": "DE"},
        {"name": "法国", "regex": r"法国|FR|France|🇫🇷", "icon_key": "FR"},
        {"name": "加拿大", "regex": r"加拿大|CA|Canada|🇨🇦", "icon_key": "CA"},
        {"name": "澳大利亚", "regex": r"澳大利亚|AU|Australia|🇦🇺", "icon_key": "AU"},
        {"name": "俄罗斯", "regex": r"俄罗斯|RU|Russia|🇷🇺", "icon_key": "RU"},
        {"name": "印度", "regex": r"印度|IN|India|🇮🇳", "icon_key": "IN"},
        {"name": "荷兰", "regex": r"荷兰|NL|Netherlands|🇳🇱", "icon_key": "NL"},
        {"name": "土耳其", "regex": r"土耳其|TR|Turkey|🇹🇷", "icon_key": "TR"},
        {"name": "巴西", "regex": r"巴西|BR|Brazil|🇧🇷", "icon_key": "BR"},
    ],
    "rules": [
        "DOMAIN,sub.datapipe.top,DIRECT",
        "DOMAIN,suc-store.usuc.cc,DIRECT",
        "DOMAIN,sub-aylz.koyeb.app,DIRECT",
        "RULE-SET,applications,DIRECT",
        "DOMAIN,clash.razord.top,DIRECT",
        "DOMAIN,yacd.haishan.me,DIRECT",
        "RULE-SET,private,DIRECT",
        "RULE-SET,reject,REJECT",
        "RULE-SET,icloud,DIRECT",
        "RULE-SET,apple,DIRECT",
        "GEOSITE,openai,AI 工具",
        "DOMAIN-SUFFIX,chatgpt.com,AI 工具",
        "DOMAIN-SUFFIX,gemini.google.com,AI 工具",
        "DOMAIN-KEYWORD,generativelanguage,AI 工具",
        "GEOSITE,anthropic,AI 工具",
        "DOMAIN-SUFFIX,claude.ai,AI 工具",
        "RULE-SET,google,节点选择",
        "RULE-SET,proxy,节点选择",
        "RULE-SET,direct,DIRECT",
        "RULE-SET,lancidr,DIRECT",
        "RULE-SET,cncidr,DIRECT",
        "RULE-SET,telegramcidr,节点选择",
        "GEOIP,LAN,DIRECT",
        "GEOIP,CN,DIRECT",
        "MATCH,节点选择",
    ],
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Use embedded subscription sources and build spider_clash.txt."
    )
    parser.add_argument(
        "--date",
        dest="target_date",
        help="Override target date in YYYY-MM-DD format. Defaults to Asia/Shanghai today.",
    )
    return parser.parse_args()


def write_text_file(file_path, content):
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(content)


def write_error_file(messages):
    content = ""
    if messages:
        content = "\n".join(messages).rstrip() + "\n"
    write_text_file(ERROR_FILE, content)


def normalize_text(value):
    return value.replace("\ufeff", "").strip()


def add_base64_padding(value):
    return value + ("=" * (-len(value) % 4))


def is_probably_base64_payload(value):
    sanitized = re.sub(r"\s+", "", value)
    if len(sanitized) < 100:
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9+/=_-]+", sanitized))


def decode_base64_text(value):
    sanitized = re.sub(r"\s+", "", value)
    padded = add_base64_padding(sanitized)
    raw_bytes = None

    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            raw_bytes = decoder(padded)
            break
        except (binascii.Error, ValueError):
            continue

    if raw_bytes is None:
        raise ValueError("Base64 decode failed")

    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return raw_bytes.decode(encoding), encoding
        except UnicodeDecodeError:
            continue

    raise ValueError("Decoded bytes could not be converted to text")


def normalize_proxy_name(name, fallback_name):
    candidate = re.sub(r"\s+", " ", str(name or fallback_name or "")).strip()
    return candidate or fallback_name


def clean_data(value):
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            cleaned_item = clean_data(item)
            if cleaned_item in (None, "", [], {}):
                continue
            cleaned[key] = cleaned_item
        return cleaned

    if isinstance(value, list):
        cleaned = []
        for item in value:
            cleaned_item = clean_data(item)
            if cleaned_item in (None, "", [], {}):
                continue
            cleaned.append(cleaned_item)
        return cleaned

    return value


def fingerprint_proxy(proxy):
    cloned = clean_data(deepcopy(proxy))
    if isinstance(cloned, dict):
        cloned.pop("name", None)
    return json.dumps(cloned, ensure_ascii=False, sort_keys=True)


def deduplicate_proxies(proxies):
    unique_proxies = []
    seen_fingerprints = set()

    for proxy in proxies:
        fingerprint = fingerprint_proxy(proxy)
        if fingerprint in seen_fingerprints:
            continue
        seen_fingerprints.add(fingerprint)
        unique_proxies.append(deepcopy(proxy))

    return unique_proxies


def ensure_unique_proxy_names(proxies):
    renamed_proxies = []
    used_names = set()
    duplicate_count = 0

    for proxy in proxies:
        cloned = deepcopy(proxy)
        base_name = normalize_proxy_name(
            cloned.get("name"),
            f"{cloned.get('type', 'proxy')}-{cloned.get('server', 'unknown')}",
        )
        candidate = base_name
        suffix = 2

        while candidate in used_names:
            candidate = f"{base_name} [{suffix}]"
            suffix += 1

        if candidate != base_name:
            duplicate_count += 1

        cloned["name"] = candidate
        used_names.add(candidate)
        renamed_proxies.append(cloned)

    return renamed_proxies, duplicate_count


def find_self_referencing_proxy_groups(config):
    loop_names = []
    for group in config.get("proxy-groups", []):
        if not isinstance(group, dict):
            continue
        group_name = group.get("name")
        group_proxies = group.get("proxies")
        if not group_name or not isinstance(group_proxies, list):
            continue
        if group_name in group_proxies:
            loop_names.append(group_name)
    return loop_names


def rename_proxies_conflicting_with_group_names(proxies, group_names):
    if not group_names:
        return [deepcopy(proxy) for proxy in proxies], 0

    reserved_names = set(group_names)
    renamed_proxies = []
    renamed_count = 0

    for proxy in proxies:
        cloned = deepcopy(proxy)
        original_name = normalize_proxy_name(
            cloned.get("name"),
            f"{cloned.get('type', 'proxy')}-{cloned.get('server', 'unknown')}",
        )

        if original_name not in reserved_names:
            renamed_proxies.append(cloned)
            continue

        suffix = 1
        candidate = f"{original_name} [node]"
        used_names = {item.get("name") for item in renamed_proxies}
        while candidate in reserved_names or candidate in used_names:
            suffix += 1
            candidate = f"{original_name} [node {suffix}]"

        cloned["name"] = candidate
        renamed_proxies.append(cloned)
        renamed_count += 1

    return renamed_proxies, renamed_count


def get_proxy_endpoint(proxy):
    server = proxy.get("server")
    port = proxy.get("port")
    proxy_type = str(proxy.get("type", "")).strip().lower()

    if not server or port in (None, ""):
        return None

    try:
        port_number = int(port)
    except (TypeError, ValueError):
        return None

    return proxy_type, str(server).strip(), port_number


def check_tcp_endpoint(endpoint):
    _, server, port = endpoint
    try:
        with socket.create_connection((server, port), timeout=REACHABILITY_TIMEOUT_SECONDS):
            return True
    except OSError:
        return False


def filter_reachable_proxies(proxies):
    endpoint_status = {}
    tcp_endpoints = []
    invalid_endpoint_count = 0

    for proxy in proxies:
        endpoint = get_proxy_endpoint(proxy)
        if endpoint is None:
            invalid_endpoint_count += 1
            continue

        if endpoint in endpoint_status:
            continue

        if endpoint[0] in UDP_ONLY_PROXY_TYPES:
            endpoint_status[endpoint] = None
            continue

        tcp_endpoints.append(endpoint)

    if tcp_endpoints:
        with ThreadPoolExecutor(max_workers=REACHABILITY_MAX_WORKERS) as executor:
            future_to_endpoint = {
                executor.submit(check_tcp_endpoint, endpoint): endpoint for endpoint in tcp_endpoints
            }
            for future in as_completed(future_to_endpoint):
                endpoint = future_to_endpoint[future]
                try:
                    endpoint_status[endpoint] = future.result()
                except Exception:
                    endpoint_status[endpoint] = False

    checked_count = sum(1 for status in endpoint_status.values() if status is not None)
    reachable_count = sum(1 for status in endpoint_status.values() if status is True)
    unchecked_count = sum(1 for status in endpoint_status.values() if status is None)

    if checked_count > 0 and reachable_count == 0:
        logger.warning(
            "Reachability filter skipped because all %s TCP probes failed; original proxies will be kept",
            checked_count,
        )
        return proxies, {
            "applied": False,
            "checked_count": checked_count,
            "reachable_count": reachable_count,
            "unchecked_count": unchecked_count,
            "dropped_count": 0,
            "invalid_endpoint_count": invalid_endpoint_count,
        }

    kept_proxies = []
    dropped_count = 0
    for proxy in proxies:
        endpoint = get_proxy_endpoint(proxy)
        if endpoint is None:
            dropped_count += 1
            continue

        status = endpoint_status.get(endpoint)
        if status is False:
            dropped_count += 1
            continue

        kept_proxies.append(proxy)

    return kept_proxies, {
        "applied": True,
        "checked_count": checked_count,
        "reachable_count": reachable_count,
        "unchecked_count": unchecked_count,
        "dropped_count": dropped_count,
        "invalid_endpoint_count": invalid_endpoint_count,
    }


def build_source_summaries(results, final_proxies):
    final_fingerprints = {fingerprint_proxy(proxy) for proxy in final_proxies}
    summaries = []

    for result in results:
        total_count = len(result["proxies"])
        if total_count == 0:
            continue

        available_fingerprints = {
            fingerprint_proxy(proxy)
            for proxy in result["proxies"]
            if fingerprint_proxy(proxy) in final_fingerprints
        }
        summaries.append(f"{result['source_name']} {len(available_fingerprints)}/{total_count}")

    return summaries


def extract_yaml_proxies(content, source_name):
    decoded_content = normalize_text(content)
    decode_mode = "plain-text"

    if is_probably_base64_payload(decoded_content):
        decoded_content, encoding = decode_base64_text(decoded_content)
        decoded_content = normalize_text(decoded_content)
        decode_mode = f"base64/{encoding}"

    try:
        data = yaml.safe_load(decoded_content)
    except yaml.YAMLError as exc:
        raise ValueError(f"PyYAML parse failed: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("Subscription content is not a YAML mapping")

    proxies = data.get("proxies")
    if not isinstance(proxies, list):
        raise ValueError("YAML does not contain a proxies list")

    normalized_proxies = []
    for proxy in proxies:
        if not isinstance(proxy, dict):
            continue
        cloned = deepcopy(proxy)
        cloned["name"] = normalize_proxy_name(
            cloned.get("name"),
            f"{source_name}-{cloned.get('type', 'proxy')}-{cloned.get('server', 'unknown')}",
        )
        cloned.setdefault("udp", True)
        normalized_proxies.append(cloned)

    if not normalized_proxies:
        raise ValueError("proxies list is empty or contains no supported entries")

    return normalized_proxies, decode_mode


def load_embedded_sources():
    return deepcopy(EMBEDDED_SOURCES)


def parse_yyyymmdd(token):
    try:
        return datetime.strptime(token, "%Y%m%d").date()
    except ValueError:
        return None


def collect_reference_dates(sources):
    dates = []
    for source in sources:
        for url in source["reference_urls"]:
            for token in re.findall(r"\d{8}", url):
                parsed = parse_yyyymmdd(token)
                if parsed is not None:
                    dates.append(parsed)
    return dates


def infer_reference_date(sources):
    dates = collect_reference_dates(sources)
    if not dates:
        inferred_today = datetime.now(UTC_PLUS_8).date()
        logger.warning(
            "Embedded sources do not contain any YYYYMMDD date token; using today=%s as anchor",
            inferred_today,
        )
        return inferred_today
    anchor = max(dates)
    logger.info("Inferred reference date from embedded sources: %s", anchor)
    return anchor


def replace_path_year_month(url, reference_file_date, target_file_date):
    parts = urlsplit(url)
    segments = parts.path.split("/")
    updated_segments = []
    reference_year = str(reference_file_date.year)
    target_year = str(target_file_date.year)

    for segment in segments:
        if re.search(r"\d{8}", segment):
            updated_segments.append(segment)
            continue

        if segment == reference_year:
            updated_segments.append(target_year)
            continue

        if segment == str(reference_file_date.month):
            updated_segments.append(str(target_file_date.month))
            continue

        if segment == f"{reference_file_date.month:02d}":
            updated_segments.append(f"{target_file_date.month:02d}")
            continue

        updated_segments.append(segment)

    return urlunsplit(parts._replace(path="/".join(updated_segments)))


def shift_reference_url(reference_url, reference_date, target_date):
    delta_days = target_date - reference_date
    detected_dates = []

    def replace_date_token(match):
        parsed = parse_yyyymmdd(match.group(0))
        if parsed is None:
            return match.group(0)
        shifted = parsed + delta_days
        detected_dates.append((parsed, shifted))
        return shifted.strftime("%Y%m%d")

    shifted_url = re.sub(r"\d{8}", replace_date_token, reference_url)
    if not detected_dates:
        return reference_url

    reference_file_date, target_file_date = detected_dates[0]
    return replace_path_year_month(shifted_url, reference_file_date, target_file_date)


def fetch_text(url):
    response = requests.get(url, headers=SUBSCRIPTION_HEADERS, timeout=30)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or "utf-8"
    return normalize_text(response.text)


def fetch_remote_main_js_source():
    response = requests.get(MAIN_JS_URL, timeout=30)
    response.raise_for_status()
    return response.text


def apply_remote_main_js(config):
    node_binary = shutil.which("node")
    if not node_binary:
        raise RuntimeError("node executable not found")

    main_js_source = fetch_remote_main_js_source()
    runner_source = "\n".join(
        [
            'const fs = require("fs");',
            'const input = fs.readFileSync(0, "utf8");',
            "const params = JSON.parse(input);",
            main_js_source,
            'if (typeof main !== "function") throw new Error("main.js does not define main(params)");',
            "const result = main(params);",
            "process.stdout.write(JSON.stringify(result ?? params));",
        ]
    )

    runner_path = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            suffix=".js",
            delete=False,
        ) as temp_file:
            temp_file.write(runner_source)
            runner_path = temp_file.name

        completed = subprocess.run(
            [node_binary, runner_path],
            input=json.dumps(config, ensure_ascii=False),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=120,
            check=False,
        )
        if completed.returncode != 0:
            stderr = normalize_text(completed.stderr)
            raise RuntimeError(
                f"node main.js execution failed with exit code {completed.returncode}: {stderr}"
            )

        stdout = normalize_text(completed.stdout)
        if not stdout:
            raise RuntimeError("node main.js execution returned empty output")

        result = json.loads(stdout)
        if not isinstance(result, dict):
            raise ValueError("main.js execution did not return a JSON object")
        required_keys = ["dns", "proxy-groups", "rule-providers", "rules"]
        missing_keys = [key for key in required_keys if key not in result]
        if missing_keys:
            raise ValueError(
                f"main.js execution result is missing required keys: {', '.join(missing_keys)}"
            )

        logger.info("Remote main.js executed successfully")
        return clean_data(result), MAIN_JS_URL, "remote-main-js-executed"
    finally:
        if runner_path and os.path.exists(runner_path):
            os.remove(runner_path)


def apply_remote_main_js_with_loop_resolution(proxies):
    current_proxies = [deepcopy(proxy) for proxy in proxies]
    total_renamed = 0

    for attempt in range(1, 4):
        base_config = build_base_config(current_proxies)
        config, rules_source, rules_mode = apply_remote_main_js(base_config)
        loop_names = find_self_referencing_proxy_groups(config)
        if not loop_names:
            return config, current_proxies, rules_source, rules_mode, total_renamed

        renamed_proxies, renamed_count = rename_proxies_conflicting_with_group_names(
            current_proxies,
            loop_names,
        )
        if renamed_count == 0:
            raise ValueError(
                f"ProxyGroup loop detected but no conflicting proxy names could be renamed: {', '.join(loop_names)}"
            )

        total_renamed += renamed_count
        current_proxies = renamed_proxies
        logger.warning(
            "ProxyGroup loop detected on remote main.js apply, auto-renamed=%s, groups=%s, attempt=%s",
            renamed_count,
            ", ".join(loop_names),
            attempt,
        )

    raise ValueError("ProxyGroup loop still exists after automatic proxy renaming")


def build_base_config(proxies):
    cleaned_proxies = [clean_data(proxy) for proxy in proxies]
    return clean_data(
        {
            "mixed-port": 7890,
            "allow-lan": False,
            "mode": "rule",
            "log-level": "info",
            "ipv6": False,
            "unified-delay": True,
            "tcp-concurrent": True,
            "profile": {
                "store-selected": True,
                "store-fake-ip": True,
            },
            "proxies": cleaned_proxies,
        }
    )


def build_icon_url(template, icon_key):
    filename = template["icons"].get(icon_key)
    if not filename:
        return ""
    return f"{template['icon_base']}{filename}"


def build_rule_providers(template):
    providers = {}
    for item in template["rule_providers"]:
        providers[item["name"]] = {
            "type": item["type"],
            "behavior": item["behavior"],
            "url": f"{template['rule_base']}{item['filename']}",
            "path": item["path"],
            "interval": item["interval"],
        }
    return providers


def build_proxy_groups(proxy_names, template):
    if not proxy_names:
        return [
            {"name": "节点选择", "type": "select", "proxies": ["DIRECT"]},
            {"name": "自动选择", "type": "select", "proxies": ["DIRECT"]},
            {"name": "手动选择", "type": "select", "proxies": ["DIRECT"]},
            {"name": "AI 工具", "type": "select", "proxies": ["DIRECT"]},
        ]

    assigned_names = set()
    active_region_groups = []
    for region in template["region_configs"]:
        try:
            regex = re.compile(region["regex"])
        except re.error as exc:
            logger.warning("Invalid region regex skipped: %s (%s)", region["name"], exc)
            continue

        matched_names = [name for name in proxy_names if regex.search(name)]
        if not matched_names:
            continue

        active_region_groups.append(
            {
                "name": region["name"],
                "type": "url-test",
                "icon": build_icon_url(template, region["icon_key"]),
                "interval": template["test_interval"],
                "url": template["test_url"],
                "tolerance": 50,
                "proxies": matched_names,
            }
        )
        assigned_names.update(matched_names)

    other_proxies = [name for name in proxy_names if name not in assigned_names]
    if other_proxies:
        active_region_groups.append(
            {
                "name": "其他",
                "type": "url-test",
                "icon": build_icon_url(template, "OTHER"),
                "interval": template["test_interval"],
                "url": template["test_url"],
                "tolerance": 50,
                "proxies": other_proxies,
            }
        )

    ai_proxies = [
        name
        for name in proxy_names
        if not re.search(r"香港|HK|CN|中国|DIRECT|直连", name, re.I)
        and name not in other_proxies
    ]
    if not ai_proxies:
        ai_proxies = ["自动选择"]

    return [
        {
            "name": "节点选择",
            "type": "select",
            "proxies": [
                "自动选择",
                *[group["name"] for group in active_region_groups],
                "手动选择",
                "DIRECT",
            ],
            "icon": build_icon_url(template, "GLOBAL"),
        },
        {
            "name": "自动选择",
            "type": "url-test",
            "proxies": proxy_names,
            "icon": build_icon_url(template, "AUTO"),
            "interval": template["test_interval"],
            "url": template["test_url"],
        },
        {
            "name": "手动选择",
            "type": "select",
            "proxies": proxy_names,
            "icon": build_icon_url(template, "MANUAL"),
        },
        {
            "name": "AI 工具",
            "type": "url-test",
            "proxies": ai_proxies,
            "icon": build_icon_url(template, "AI"),
            "interval": template["test_interval"],
            "url": template["test_url"],
        },
        *active_region_groups,
    ]


def build_clash_config(proxies, template):
    cleaned_proxies = [clean_data(proxy) for proxy in proxies]
    proxy_names = [proxy["name"] for proxy in cleaned_proxies]

    return clean_data(
        {
            "mixed-port": 7890,
            "allow-lan": False,
            "mode": "rule",
            "log-level": "info",
            "ipv6": False,
            "unified-delay": True,
            "tcp-concurrent": True,
            "profile": {
                "store-selected": True,
                "store-fake-ip": True,
            },
            "dns": deepcopy(template["dns"]),
            "proxies": cleaned_proxies,
            "proxy-groups": build_proxy_groups(proxy_names, template),
            "rule-providers": build_rule_providers(template),
            "rules": template["rules"],
        }
    )


def build_fallback_config_with_loop_resolution(proxies, template):
    current_proxies = [deepcopy(proxy) for proxy in proxies]
    total_renamed = 0

    for attempt in range(1, 4):
        config = build_clash_config(current_proxies, template)
        loop_names = find_self_referencing_proxy_groups(config)
        if not loop_names:
            return config, current_proxies, total_renamed

        renamed_proxies, renamed_count = rename_proxies_conflicting_with_group_names(
            current_proxies,
            loop_names,
        )
        if renamed_count == 0:
            raise ValueError(
                f"Fallback ProxyGroup loop detected but no conflicting proxy names could be renamed: {', '.join(loop_names)}"
            )

        total_renamed += renamed_count
        current_proxies = renamed_proxies
        logger.warning(
            "ProxyGroup loop detected on fallback config build, auto-renamed=%s, groups=%s, attempt=%s",
            renamed_count,
            ", ".join(loop_names),
            attempt,
        )

    raise ValueError("Fallback ProxyGroup loop still exists after automatic proxy renaming")


def build_clash_file_text(
    config,
    results,
    source_filename,
    reference_date,
    target_date,
    rules_source,
    rules_mode,
    source_summaries,
):
    generated_at = datetime.now(UTC_PLUS_8).isoformat(timespec="seconds")
    header = "\n".join(
        [
            f"# generated_at: {generated_at}",
            f"# source_file: {source_filename}",
            f"# reference_date: {reference_date.isoformat()}",
            f"# effective_date: {target_date.isoformat()}",
            f"# rules_source: {rules_source}",
            f"# rules_mode: {rules_mode}",
            f"# success_sources: {', '.join(source_summaries) if source_summaries else 'none'}",
            "",
        ]
    )
    body = yaml.safe_dump(
        config,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )
    return header + body


def process_source(source, reference_date, target_date):
    source_name = source["source_name"]
    result = {
        "source_name": source_name,
        "reference_urls": source["reference_urls"],
        "runtime_urls": [],
        "proxies": [],
        "errors": [],
    }

    logger.info("[%s] Start source, reference_urls=%s", source_name, len(source["reference_urls"]))
    for index, reference_url in enumerate(source["reference_urls"], start=1):
        runtime_url = shift_reference_url(reference_url, reference_date, target_date)
        result["runtime_urls"].append(runtime_url)
        logger.info(
            "[%s] URL %s/%s -> %s",
            source_name,
            index,
            len(source["reference_urls"]),
            runtime_url,
        )

        try:
            content = fetch_text(runtime_url)
            proxies, decode_mode = extract_yaml_proxies(content, source_name)
            result["proxies"].extend(proxies)
            logger.info(
                "[%s] URL %s/%s success, decode=%s, proxies=%s",
                source_name,
                index,
                len(source["reference_urls"]),
                decode_mode,
                len(proxies),
            )
        except Exception as exc:
            message = f"{runtime_url} -> {type(exc).__name__}: {exc}"
            result["errors"].append(message)
            logger.error("[%s] URL %s/%s failed: %s", source_name, index, len(source["reference_urls"]), message)

    logger.info(
        "[%s] Source completed, collected_proxies=%s, failures=%s",
        source_name,
        len(result["proxies"]),
        len(result["errors"]),
    )
    return result


def generate_spider_clash_file(results, source_filename, reference_date, target_date):
    logger.info("Start generating spider_clash.txt")
    generation_errors = []

    collected_proxies = []
    for result in results:
        collected_proxies.extend(result["proxies"])

    unique_proxies = deduplicate_proxies(collected_proxies)
    logger.info(
        "Proxy aggregation completed: raw=%s, deduplicated=%s",
        len(collected_proxies),
        len(unique_proxies),
    )
    if not unique_proxies:
        logger.warning("No proxies were extracted; spider_clash.txt will contain DIRECT-only fallback groups")

    reachable_proxies, reachability_stats = filter_reachable_proxies(unique_proxies)
    logger.info(
        "Reachability filter: applied=%s, checked=%s, reachable=%s, unchecked=%s, dropped=%s, invalid_endpoint=%s, remaining=%s",
        reachability_stats["applied"],
        reachability_stats["checked_count"],
        reachability_stats["reachable_count"],
        reachability_stats["unchecked_count"],
        reachability_stats["dropped_count"],
        reachability_stats["invalid_endpoint_count"],
        len(reachable_proxies),
    )
    if not reachable_proxies:
        logger.warning("No proxies remained after reachability filtering; original deduplicated proxies will be restored")
        reachable_proxies = unique_proxies

    named_proxies, duplicate_name_count = ensure_unique_proxy_names(reachable_proxies)
    if duplicate_name_count:
        logger.info("Duplicate proxy names detected and renamed: %s", duplicate_name_count)

    final_proxies = named_proxies
    rules_source = MAIN_JS_URL
    rules_mode = "remote-main-js-executed"

    try:
        config, final_proxies, rules_source, rules_mode, loop_renamed_count = (
            apply_remote_main_js_with_loop_resolution(named_proxies)
        )
        if loop_renamed_count:
            logger.info(
                "Proxy names auto-renamed to resolve ProxyGroup loops: %s",
                loop_renamed_count,
            )
    except Exception as exc:
        message = f"main.js -> {type(exc).__name__}: {exc}"
        generation_errors.append(message)
        logger.error("Remote main.js apply failed, fallback will be used: %s", message)
        config, final_proxies, fallback_loop_renamed_count = build_fallback_config_with_loop_resolution(
            named_proxies,
            FALLBACK_OVERSEER_TEMPLATE,
        )
        if fallback_loop_renamed_count:
            logger.info(
                "Proxy names auto-renamed to resolve fallback ProxyGroup loops: %s",
                fallback_loop_renamed_count,
            )
        rules_mode = "fallback-default"

    source_summaries = build_source_summaries(results, final_proxies)

    output_text = build_clash_file_text(
        config,
        results,
        source_filename,
        reference_date,
        target_date,
        rules_source,
        rules_mode,
        source_summaries,
    )
    write_text_file(OUTPUT_FILE, output_text)
    logger.info("Clash configuration written to %s", OUTPUT_FILE)
    return generation_errors


def main():
    args = parse_args()
    target_date = (
        date.fromisoformat(args.target_date)
        if args.target_date
        else datetime.now(UTC_PLUS_8).date()
    )
    write_error_file([])

    logger.info("Task start: source=%s, target_date=%s", EMBEDDED_SOURCE_LABEL, target_date)
    sources = load_embedded_sources()
    reference_date = infer_reference_date(sources)
    logger.info(
        "Parsed source groups=%s, reference_date=%s, delta_days=%s",
        len(sources),
        reference_date,
        (target_date - reference_date).days,
    )

    results = []
    for source in sources:
        results.append(process_source(source, reference_date, target_date))

    generation_errors = generate_spider_clash_file(
        results,
        EMBEDDED_SOURCE_LABEL,
        reference_date,
        target_date,
    )

    success_sources = sum(1 for result in results if result["proxies"])
    failed_urls = sum(len(result["errors"]) for result in results)
    all_errors = []
    for result in results:
        all_errors.extend(result["errors"])
    all_errors.extend(generation_errors)
    write_error_file(all_errors)
    logger.info(
        "Error file updated: %s, entries=%s",
        ERROR_FILE,
        len(all_errors),
    )
    logger.info(
        "Task finished: sources=%s, success_sources=%s, failed_urls=%s, output=%s",
        len(results),
        success_sources,
        failed_urls,
        OUTPUT_FILE,
    )


if __name__ == "__main__":
    main()
