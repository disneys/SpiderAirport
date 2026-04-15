import argparse
import base64
import binascii
import hashlib
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
MAX_404_LOOKBACK_DAYS = 7
ALWAYS_REFRESH_SOURCES = {
    "chengaopan/AutoMergePublicNodes",
    "peasoft/NoMoreWalls",
}
SOURCE_ENABLED = {
    "clashnodev2ray.github.io": True,
    "clashmetagithub.github.io": True,
    "freev2rayclash.github.io": True,
    "stashgithub.github.io": True,
    "windowsclashnode.github.io": True,
    "clashnode.github.io": True,
    "clashvergerev.github.io": True,
    "clash-verge.github.io": True,
    "vlessgithub.github.io": True,
    "node.freeclashnode.com": True,
    "chengaopan/AutoMergePublicNodes": True,
    "peasoft/NoMoreWalls": True,
}
EMBEDDED_SOURCES = [
    {
        "source_name": "clashnodev2ray.github.io",
        "reference_urls": [
            "https://sfdr.zaixianyouxi.dpdns.org/uploads/{yyyy}/{M}/{yyyyMMdd}.yaml",
        ],
    },
    {
        "source_name": "clashmetagithub.github.io",
        "reference_urls": [
            "https://clashmetagithub.github.io/uploads/{yyyy}/{MM}/0-{yyyyMMdd}.yaml",
            "https://clashmetagithub.github.io/uploads/{yyyy}/{MM}/1-{yyyyMMdd}.yaml",
            "https://clashmetagithub.github.io/uploads/{yyyy}/{MM}/2-{yyyyMMdd}.yaml",
            "https://clashmetagithub.github.io/uploads/{yyyy}/{MM}/3-{yyyyMMdd}.yaml",
            "https://clashmetagithub.github.io/uploads/{yyyy}/{MM}/4-{yyyyMMdd}.yaml",
        ],
    },
    {
        "source_name": "freev2rayclash.github.io",
        "reference_urls": [
            "https://freev2rayclash.github.io/uploads/{yyyy}/{MM}/0-{yyyyMMdd}.yaml",
            "https://freev2rayclash.github.io/uploads/{yyyy}/{MM}/1-{yyyyMMdd}.yaml",
            "https://freev2rayclash.github.io/uploads/{yyyy}/{MM}/2-{yyyyMMdd}.yaml",
            "https://freev2rayclash.github.io/uploads/{yyyy}/{MM}/3-{yyyyMMdd}.yaml",
            "https://freev2rayclash.github.io/uploads/{yyyy}/{MM}/4-{yyyyMMdd}.yaml",
        ],
    },
    {
        "source_name": "stashgithub.github.io",
        "reference_urls": [
            "https://stashgithub.github.io/uploads/{yyyy}/{MM}/0-{yyyyMMdd}.yaml",
            "https://stashgithub.github.io/uploads/{yyyy}/{MM}/1-{yyyyMMdd}.yaml",
            "https://stashgithub.github.io/uploads/{yyyy}/{MM}/2-{yyyyMMdd}.yaml",
            "https://stashgithub.github.io/uploads/{yyyy}/{MM}/3-{yyyyMMdd}.yaml",
            "https://stashgithub.github.io/uploads/{yyyy}/{MM}/4-{yyyyMMdd}.yaml",
        ],
    },
    {
        "source_name": "windowsclashnode.github.io",
        "reference_urls": [
            "https://windowsclashnode.github.io/uploads/{yyyy}/{MM}/0-{yyyyMMdd}.yaml",
            "https://windowsclashnode.github.io/uploads/{yyyy}/{MM}/1-{yyyyMMdd}.yaml",
            "https://windowsclashnode.github.io/uploads/{yyyy}/{MM}/2-{yyyyMMdd}.yaml",
            "https://windowsclashnode.github.io/uploads/{yyyy}/{MM}/3-{yyyyMMdd}.yaml",
            "https://windowsclashnode.github.io/uploads/{yyyy}/{MM}/4-{yyyyMMdd}.yaml",
        ],
    },
    {
        "source_name": "clashnode.github.io",
        "reference_urls": [
            "https://clashnode.github.io/uploads/{yyyy}/{MM}/0-{yyyyMMdd}.yaml",
            "https://clashnode.github.io/uploads/{yyyy}/{MM}/1-{yyyyMMdd}.yaml",
            "https://clashnode.github.io/uploads/{yyyy}/{MM}/2-{yyyyMMdd}.yaml",
            "https://clashnode.github.io/uploads/{yyyy}/{MM}/3-{yyyyMMdd}.yaml",
            "https://clashnode.github.io/uploads/{yyyy}/{MM}/4-{yyyyMMdd}.yaml",
        ],
    },
    {
        "source_name": "clashvergerev.github.io",
        "reference_urls": [
            "https://clashvergerev.github.io/uploads/{yyyy}/{MM}/0-{yyyyMMdd}.yaml",
            "https://clashvergerev.github.io/uploads/{yyyy}/{MM}/1-{yyyyMMdd}.yaml",
            "https://clashvergerev.github.io/uploads/{yyyy}/{MM}/2-{yyyyMMdd}.yaml",
            "https://clashvergerev.github.io/uploads/{yyyy}/{MM}/3-{yyyyMMdd}.yaml",
            "https://clashvergerev.github.io/uploads/{yyyy}/{MM}/4-{yyyyMMdd}.yaml",
        ],
    },
    {
        "source_name": "clash-verge.github.io",
        "reference_urls": [
            "https://clash-verge.github.io/uploads/{yyyy}/{MM}/0-{yyyyMMdd}.yaml",
            "https://clash-verge.github.io/uploads/{yyyy}/{MM}/1-{yyyyMMdd}.yaml",
            "https://clash-verge.github.io/uploads/{yyyy}/{MM}/2-{yyyyMMdd}.yaml",
            "https://clash-verge.github.io/uploads/{yyyy}/{MM}/3-{yyyyMMdd}.yaml",
            "https://clash-verge.github.io/uploads/{yyyy}/{MM}/4-{yyyyMMdd}.yaml",
        ],
    },
    {
        "source_name": "vlessgithub.github.io",
        "reference_urls": [
            "https://vlessgithub.github.io/uploads/{yyyy}/{MM}/0-{yyyyMMdd}.yaml",
            "https://vlessgithub.github.io/uploads/{yyyy}/{MM}/1-{yyyyMMdd}.yaml",
            "https://vlessgithub.github.io/uploads/{yyyy}/{MM}/2-{yyyyMMdd}.yaml",
            "https://vlessgithub.github.io/uploads/{yyyy}/{MM}/3-{yyyyMMdd}.yaml",
            "https://vlessgithub.github.io/uploads/{yyyy}/{MM}/4-{yyyyMMdd}.yaml",
        ],
    },
    {
        "source_name": "node.freeclashnode.com",
        "reference_urls": [
            "https://node.freeclashnode.com/uploads/{yyyy}/{MM}/0-{yyyyMMdd}.yaml",
            "https://node.freeclashnode.com/uploads/{yyyy}/{MM}/1-{yyyyMMdd}.yaml",
            "https://node.freeclashnode.com/uploads/{yyyy}/{MM}/2-{yyyyMMdd}.yaml",
            "https://node.freeclashnode.com/uploads/{yyyy}/{MM}/3-{yyyyMMdd}.yaml",
            "https://node.freeclashnode.com/uploads/{yyyy}/{MM}/4-{yyyyMMdd}.yaml",
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
        description="使用内嵌订阅源生成 spider_clash.txt。"
    )
    parser.add_argument(
        "--date",
        dest="target_date",
        help="指定目标日期，格式为 YYYY-MM-DD。默认使用 Asia/Shanghai 的当天日期。",
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
        raise ValueError("Base64 解码失败")

    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return raw_bytes.decode(encoding), encoding
        except UnicodeDecodeError:
            continue

    raise ValueError("解码后的字节内容无法转换为文本")


def normalize_proxy_name(name, fallback_name):
    candidate = re.sub(r"\s+", " ", str(name or fallback_name or "")).strip()
    return candidate or fallback_name


def clean_data(value):
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if str(key).startswith("__"):
                continue
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
        cloned.pop("__sources", None)
    return json.dumps(cloned, ensure_ascii=False, sort_keys=True)


def proxy_digest(proxy):
    return hashlib.sha1(fingerprint_proxy(proxy).encode("utf-8")).hexdigest()[:16]


def deduplicate_proxies(proxies):
    unique_proxies = []
    fingerprint_to_index = {}

    for proxy in proxies:
        fingerprint = fingerprint_proxy(proxy)
        if fingerprint in fingerprint_to_index:
            existing_proxy = unique_proxies[fingerprint_to_index[fingerprint]]
            existing_sources = set(existing_proxy.get("__sources", []))
            existing_sources.update(proxy.get("__sources", []))
            existing_proxy["__sources"] = sorted(existing_sources)
            continue
        fingerprint_to_index[fingerprint] = len(unique_proxies)
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


def build_proxy_source_label(proxy):
    sources = sorted(set(proxy.get("__sources", [])))
    if not sources:
        return ""
    return "|".join(sources)


def strip_source_tag(name):
    return re.sub(r"\s+\{src=[^}]+\}", "", str(name)).strip()


def extract_source_names_from_proxy_name(name):
    match = re.search(r"\s+\{src=([^}]+)\}", str(name))
    if not match:
        return []
    return [item for item in match.group(1).split("|") if item]


def decorate_proxy_names_with_source_tags(proxies):
    decorated_proxies = []
    decorated_count = 0

    for proxy in proxies:
        cloned = deepcopy(proxy)
        source_label = build_proxy_source_label(cloned)
        if not source_label:
            decorated_proxies.append(cloned)
            continue

        base_name = normalize_proxy_name(
            strip_source_tag(cloned.get("name")),
            f"{cloned.get('type', 'proxy')}-{cloned.get('server', 'unknown')}",
        )
        decorated_name = f"{base_name} {{src={source_label}}}"
        if cloned.get("name") != decorated_name:
            cloned["name"] = decorated_name
            decorated_count += 1
        decorated_proxies.append(cloned)

    return decorated_proxies, decorated_count


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
            "可达性过滤已跳过：全部 %s 个 TCP 探测都失败，保留原始去重后的节点",
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
        if result.get("fetch_mode") == "disabled":
            summaries.append(f"{result['source_name']} -/- -")
            continue

        if result.get("fetch_mode") == "reused-cache":
            summaries.append(
                f"{result['source_name']} {result.get('summary_available_count', 0)}/{result.get('summary_total_count', 0)} 200"
            )
            continue

        total_count = len(result["proxies"])
        available_fingerprints = {
            fingerprint_proxy(proxy)
            for proxy in result["proxies"]
            if fingerprint_proxy(proxy) in final_fingerprints
        }
        available_count = len(available_fingerprints)
        status_code = "200" if available_count > 0 else "404"
        summaries.append(f"{result['source_name']} {available_count}/{total_count} {status_code}")

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
        raise ValueError(f"PyYAML 解析失败：{exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("订阅内容不是 YAML 字典结构")

    proxies = data.get("proxies")
    if not isinstance(proxies, list):
        raise ValueError("YAML 中未找到 proxies 列表")

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
        cloned["__sources"] = [source_name]
        normalized_proxies.append(cloned)

    if not normalized_proxies:
        raise ValueError("proxies 列表为空，或不包含可用节点")

    return normalized_proxies, decode_mode


def load_embedded_sources():
    return deepcopy(EMBEDDED_SOURCES)


def is_source_enabled(source_name):
    return SOURCE_ENABLED.get(source_name, True)


def build_date_template_values(target_date):
    return {
        "yyyy": f"{target_date.year:04d}",
        "MM": f"{target_date.month:02d}",
        "M": str(target_date.month),
        "dd": f"{target_date.day:02d}",
        "yyyyMMdd": target_date.strftime("%Y%m%d"),
    }


def infer_reference_date(sources, target_date):
    logger.info("模板日期直接使用目标日期：%s", target_date)
    return target_date


def infer_source_reference_date(source, target_date):
    logger.info("[%s] 模板日期直接使用目标日期：%s", source["source_name"], target_date)
    return target_date


def render_reference_url(reference_url, target_date):
    values = build_date_template_values(target_date)
    rendered = reference_url
    for key in ("yyyyMMdd", "yyyy", "MM", "M", "dd"):
        rendered = rendered.replace(f"{{{key}}}", values[key])
    return rendered, values


def is_http_404_error(exc):
    return (
        isinstance(exc, requests.HTTPError)
        and exc.response is not None
        and exc.response.status_code == 404
    )


def parse_comment_header(file_path):
    if not os.path.exists(file_path):
        return {}

    header = {}
    with open(file_path, "r", encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.rstrip("\n")
            if not line.startswith("# "):
                break
            content = line[2:]
            if ": " not in content:
                continue
            key, value = content.split(": ", 1)
            header[key.strip()] = value.strip()
    return header


def parse_output_config(file_path):
    if not os.path.exists(file_path):
        return {}

    with open(file_path, "r", encoding="utf-8") as file:
        body_lines = [line for line in file if not line.startswith("# ")]

    body_text = "".join(body_lines).strip()
    if not body_text:
        return {}

    try:
        config = yaml.safe_load(body_text)
    except yaml.YAMLError as exc:
        logger.warning("解析现有 spider_clash.txt 正文失败：%s", exc)
        return {}

    return config if isinstance(config, dict) else {}


def extract_source_proxies_from_config(existing_config, source_name):
    proxies = []
    for proxy in existing_config.get("proxies", []):
        if not isinstance(proxy, dict):
            continue
        source_names = extract_source_names_from_proxy_name(proxy.get("name", ""))
        if source_name not in source_names:
            continue
        cloned = deepcopy(proxy)
        cloned["name"] = strip_source_tag(cloned.get("name", ""))
        cloned["__sources"] = source_names or [source_name]
        proxies.append(cloned)
    return proxies


def parse_source_summary_line(summary_line):
    summaries = {}
    if not summary_line or summary_line == "none":
        return summaries

    for item in summary_line.split(", "):
        disabled_match = re.fullmatch(r"(.+?)\s+-/-\s+-", item.strip())
        if disabled_match:
            source_name = disabled_match.group(1)
            summaries[source_name] = {
                "available_count": None,
                "total_count": None,
                "disabled": True,
                "status_code": "-",
            }
            continue

        match = re.fullmatch(r"(.+?)\s+(\d+)/(\d+)\s+(200|404)", item.strip())
        if not match:
            continue
        source_name, available_count, total_count, status_code = match.groups()
        summaries[source_name] = {
            "available_count": int(available_count),
            "total_count": int(total_count),
            "disabled": False,
            "status_code": status_code,
        }
    return summaries


def load_existing_run_metadata(target_date):
    header = parse_comment_header(OUTPUT_FILE)
    existing_config = parse_output_config(OUTPUT_FILE)
    metadata = {
        "effective_date": None,
        "source_summaries": {},
        "existing_config": existing_config,
        "is_current_day": False,
    }

    effective_date = header.get("effective_date")
    if effective_date:
        try:
            metadata["effective_date"] = date.fromisoformat(effective_date)
        except ValueError:
            metadata["effective_date"] = None

    metadata["source_summaries"] = parse_source_summary_line(header.get("success_sources", ""))
    metadata["is_current_day"] = metadata["effective_date"] == target_date
    return metadata


def should_fetch_source(source_name, target_date, existing_metadata):
    if source_name in ALWAYS_REFRESH_SOURCES:
        return True

    if not existing_metadata["is_current_day"]:
        return True

    summary = existing_metadata["source_summaries"].get(source_name)
    if summary is None:
        return True

    if summary.get("disabled"):
        return False

    if summary["available_count"] == 0:
        return True

    cached_proxies = extract_source_proxies_from_config(existing_metadata["existing_config"], source_name)
    if not cached_proxies:
        return True

    return False


def build_cached_result(source, target_date, existing_metadata):
    source_name = source["source_name"]
    summary = existing_metadata["source_summaries"].get(
        source_name,
        {"available_count": 0, "total_count": 0, "disabled": False},
    )
    cached_proxies = extract_source_proxies_from_config(existing_metadata["existing_config"], source_name)
    result = {
        "source_name": source_name,
        "reference_urls": source["reference_urls"],
        "runtime_urls": [],
        "proxies": cached_proxies,
        "errors": [],
        "source_reference_date": infer_source_reference_date(source, target_date),
        "fetch_mode": "reused-cache",
        "summary_available_count": summary.get("available_count", 0) or 0,
        "summary_total_count": summary.get("total_count", 0) or 0,
    }
    logger.info(
        "[%s] 今天已获取到有效节点，跳过抓取并复用现有配置中的节点，数量=%s",
        source_name,
        len(result["proxies"]),
    )
    return result


def build_disabled_result(source, target_date):
    source_name = source["source_name"]
    result = {
        "source_name": source_name,
        "reference_urls": source["reference_urls"],
        "runtime_urls": [],
        "proxies": [],
        "errors": [],
        "source_reference_date": infer_source_reference_date(source, target_date),
        "fetch_mode": "disabled",
    }
    logger.info("[%s] 已被 SOURCE_ENABLED 开关禁用，跳过抓取", source_name)
    return result


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
        raise RuntimeError("未找到 node 可执行文件")

    main_js_source = fetch_remote_main_js_source()
    runner_source = "\n".join(
        [
            'const fs = require("fs");',
            'const input = fs.readFileSync(0, "utf8");',
            "const params = JSON.parse(input);",
            main_js_source,
            'if (typeof main !== "function") throw new Error("main.js 未定义 main(params) 函数");',
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
                f"node 执行 main.js 失败，退出码={completed.returncode}：{stderr}"
            )

        stdout = normalize_text(completed.stdout)
        if not stdout:
            raise RuntimeError("node 执行 main.js 后返回了空输出")

        result = json.loads(stdout)
        if not isinstance(result, dict):
            raise ValueError("main.js 执行结果不是 JSON 对象")
        required_keys = ["dns", "proxy-groups", "rule-providers", "rules"]
        missing_keys = [key for key in required_keys if key not in result]
        if missing_keys:
            raise ValueError(
                f"main.js 执行结果缺少必要字段：{', '.join(missing_keys)}"
            )

        logger.info("远程 main.js 执行成功")
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
                f"检测到 ProxyGroup 自环，但无法重命名与分组重名的节点：{', '.join(loop_names)}"
            )

        total_renamed += renamed_count
        current_proxies = renamed_proxies
        logger.warning(
            "远程 main.js 生成的 ProxyGroup 存在自环，已自动重命名 %s 个冲突节点，分组=%s，重试次数=%s",
            renamed_count,
            ", ".join(loop_names),
            attempt,
        )

    raise ValueError("自动重命名冲突节点后，ProxyGroup 自环仍然存在")


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
            logger.warning("区域分组正则无效，已跳过：%s（%s）", region["name"], exc)
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
                f"回退配置存在 ProxyGroup 自环，但无法重命名与分组重名的节点：{', '.join(loop_names)}"
            )

        total_renamed += renamed_count
        current_proxies = renamed_proxies
        logger.warning(
            "回退配置生成的 ProxyGroup 存在自环，已自动重命名 %s 个冲突节点，分组=%s，重试次数=%s",
            renamed_count,
            ", ".join(loop_names),
            attempt,
        )

    raise ValueError("回退配置在自动重命名冲突节点后，ProxyGroup 自环仍然存在")


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
    header_lines = [
        f"# generated_at: {generated_at}",
        f"# source_file: {source_filename}",
        f"# reference_date: {reference_date.isoformat()}",
        f"# effective_date: {target_date.isoformat()}",
        f"# rules_source: {rules_source}",
        f"# rules_mode: {rules_mode}",
        f"# success_sources: {', '.join(source_summaries) if source_summaries else 'none'}",
    ]
    header_lines.append("")
    header = "\n".join(header_lines)
    body = yaml.safe_dump(
        config,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )
    return header + body


def process_source(source, reference_date, target_date):
    source_name = source["source_name"]
    source_reference_date = infer_source_reference_date(source, target_date)
    template_values = build_date_template_values(target_date)
    result = {
        "source_name": source_name,
        "reference_urls": source["reference_urls"],
        "runtime_urls": [],
        "proxies": [],
        "errors": [],
        "source_reference_date": source_reference_date,
        "fetch_mode": "fetched",
    }

    logger.info(
        "[%s] 开始处理，模板链接数=%s，目标日期=%s，404 最多向前回退=%s 天，日期变量：yyyy=%s，MM=%s，M=%s，yyyyMMdd=%s",
        source_name,
        len(source["reference_urls"]),
        target_date,
        MAX_404_LOOKBACK_DAYS,
        template_values["yyyy"],
        template_values["MM"],
        template_values["M"],
        template_values["yyyyMMdd"],
    )
    for index, reference_url in enumerate(source["reference_urls"], start=1):
        found_config = False

        for fallback_days in range(MAX_404_LOOKBACK_DAYS + 1):
            attempt_date = target_date - timedelta(days=fallback_days)
            runtime_url, render_values = render_reference_url(reference_url, attempt_date)
            result["runtime_urls"].append(runtime_url)
            logger.info(
                "[%s] 链接 %s/%s 第 %s 次尝试：模板=%s，尝试日期=%s，回退=%s 天，替换后 yyyy=%s，MM=%s，M=%s，yyyyMMdd=%s，实际请求=%s",
                source_name,
                index,
                len(source["reference_urls"]),
                fallback_days + 1,
                reference_url,
                attempt_date,
                fallback_days,
                render_values["yyyy"],
                render_values["MM"],
                render_values["M"],
                render_values["yyyyMMdd"],
                runtime_url,
            )

            try:
                content = fetch_text(runtime_url)
                proxies, decode_mode = extract_yaml_proxies(content, source_name)
                result["proxies"].extend(proxies)
                logger.info(
                    "[%s] 链接 %s/%s 第 %s 次尝试成功，命中日期=%s，解码方式=%s，节点数=%s",
                    source_name,
                    index,
                    len(source["reference_urls"]),
                    fallback_days + 1,
                    attempt_date,
                    decode_mode,
                    len(proxies),
                )
                found_config = True
                break
            except Exception as exc:
                if is_http_404_error(exc):
                    if fallback_days < MAX_404_LOOKBACK_DAYS:
                        logger.warning(
                            "[%s] 链接 %s/%s 第 %s 次尝试返回 404，继续向前回退 1 天后重试",
                            source_name,
                            index,
                            len(source["reference_urls"]),
                            fallback_days + 1,
                        )
                        continue

                    message = (
                        f"{runtime_url} -> HTTP 404：已从 {target_date.isoformat()} "
                        f"连续向前回退 {MAX_404_LOOKBACK_DAYS} 天，直到 "
                        f"{attempt_date.isoformat()} 仍未找到可用配置文件"
                    )
                    result["errors"].append(message)
                    logger.error(
                        "[%s] 链接 %s/%s 连续回退 %s 天后仍然是 404，最后尝试日期=%s，最后请求=%s",
                        source_name,
                        index,
                        len(source["reference_urls"]),
                        MAX_404_LOOKBACK_DAYS,
                        attempt_date,
                        runtime_url,
                    )
                    break

                message = f"{runtime_url} -> {type(exc).__name__}: {exc}"
                result["errors"].append(message)
                logger.error(
                    "[%s] 链接 %s/%s 第 %s 次尝试失败：%s",
                    source_name,
                    index,
                    len(source["reference_urls"]),
                    fallback_days + 1,
                    message,
                )
                break

        if not found_config:
            logger.info(
                "[%s] 链接 %s/%s 未找到可用配置文件或解析失败，继续处理下一条模板链接",
                source_name,
                index,
                len(source["reference_urls"]),
            )

    logger.info(
        "[%s] 分组处理完成，收集到节点=%s，失败数=%s",
        source_name,
        len(result["proxies"]),
        len(result["errors"]),
    )
    return result


def generate_spider_clash_file(results, source_filename, reference_date, target_date):
    logger.info("开始生成 spider_clash.txt")
    generation_errors = []

    collected_proxies = []
    for result in results:
        collected_proxies.extend(result["proxies"])

    unique_proxies = deduplicate_proxies(collected_proxies)
    logger.info(
        "节点汇总完成：原始节点=%s，去重后=%s",
        len(collected_proxies),
        len(unique_proxies),
    )
    if not unique_proxies:
        logger.warning("未提取到任何节点，spider_clash.txt 将仅生成 DIRECT 回退分组")

    reachable_proxies, reachability_stats = filter_reachable_proxies(unique_proxies)
    logger.info(
        "可达性过滤结果：已应用=%s，检测=%s，可达=%s，未检测=%s，剔除=%s，无效端点=%s，剩余=%s",
        reachability_stats["applied"],
        reachability_stats["checked_count"],
        reachability_stats["reachable_count"],
        reachability_stats["unchecked_count"],
        reachability_stats["dropped_count"],
        reachability_stats["invalid_endpoint_count"],
        len(reachable_proxies),
    )
    if not reachable_proxies:
        logger.warning("可达性过滤后没有剩余节点，恢复使用去重后的原始节点")
        reachable_proxies = unique_proxies

    tagged_proxies, tagged_count = decorate_proxy_names_with_source_tags(reachable_proxies)
    if tagged_count:
        logger.info("已为节点名称追加来源标签，数量=%s", tagged_count)

    named_proxies, duplicate_name_count = ensure_unique_proxy_names(tagged_proxies)
    if duplicate_name_count:
        logger.info("检测到重名节点，已自动重命名数量=%s", duplicate_name_count)

    final_proxies = named_proxies
    rules_source = MAIN_JS_URL
    rules_mode = "remote-main-js-executed"

    try:
        config, final_proxies, rules_source, rules_mode, loop_renamed_count = (
            apply_remote_main_js_with_loop_resolution(named_proxies)
        )
        if loop_renamed_count:
            logger.info(
                "为消除 ProxyGroup 自环，已自动重命名节点数量=%s",
                loop_renamed_count,
            )
    except Exception as exc:
        message = f"main.js -> {type(exc).__name__}: {exc}"
        generation_errors.append(message)
        logger.error("远程 main.js 应用失败，改用本地回退规则：%s", message)
        config, final_proxies, fallback_loop_renamed_count = build_fallback_config_with_loop_resolution(
            named_proxies,
            FALLBACK_OVERSEER_TEMPLATE,
        )
        if fallback_loop_renamed_count:
            logger.info(
                "为消除回退配置的 ProxyGroup 自环，已自动重命名节点数量=%s",
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
    logger.info("已写入 Clash 配置文件：%s", OUTPUT_FILE)
    return generation_errors


def main():
    args = parse_args()
    target_date = (
        date.fromisoformat(args.target_date)
        if args.target_date
        else datetime.now(UTC_PLUS_8).date()
    )
    write_error_file([])

    logger.info("任务开始：来源=%s，目标日期=%s", EMBEDDED_SOURCE_LABEL, target_date)
    sources = load_embedded_sources()
    reference_date = infer_reference_date(sources, target_date)
    existing_metadata = load_existing_run_metadata(target_date)
    logger.info(
        "已解析分组数=%s，模板日期=%s，日期偏移=%s，是否已存在当天结果=%s",
        len(sources),
        reference_date,
        (target_date - reference_date).days,
        existing_metadata["is_current_day"],
    )

    results = []
    for source in sources:
        source_name = source["source_name"]
        if not is_source_enabled(source_name):
            results.append(build_disabled_result(source, target_date))
        elif should_fetch_source(source_name, target_date, existing_metadata):
            results.append(process_source(source, reference_date, target_date))
        else:
            results.append(build_cached_result(source, target_date, existing_metadata))

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
        "错误文件已更新：%s，记录数=%s",
        ERROR_FILE,
        len(all_errors),
    )
    logger.info(
        "任务结束：分组总数=%s，成功分组=%s，失败链接=%s，输出文件=%s",
        len(results),
        success_sources,
        failed_urls,
        OUTPUT_FILE,
    )


if __name__ == "__main__":
    main()
