import argparse
import base64
import binascii
import json
import logging
import os
import re
from copy import deepcopy
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
SUBSCRIPTION_HEADERS = {"User-Agent": "clash-verge/v2.4.7"}
MAIN_JS_URL = (
    "https://raw.githubusercontent.com/disneys/"
    "Mihomo-Dynamic-Overseer/refs/heads/main/main.js"
)
UTC_PLUS_8 = timezone(timedelta(hours=8))
EMBEDDED_SOURCE_LABEL = "embedded_sources"
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
    return json.dumps(clean_data(deepcopy(proxy)), ensure_ascii=False, sort_keys=True)


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


def extract_js_string_constant(source, constant_name):
    match = re.search(rf'const\s+{constant_name}\s*=\s*"([^"]+)"', source)
    return match.group(1) if match else ""


def extract_js_number_constant(source, constant_name):
    match = re.search(rf"const\s+{constant_name}\s*=\s*(\d+)", source)
    return int(match.group(1)) if match else None


def extract_js_icon_mapping(source):
    match = re.search(r"const\s+ICON\s*=\s*\{(.*?)\};", source, re.S)
    if not match:
        return {}

    icon_mapping = {}
    for key, filename in re.findall(
        r'([A-Z]+)\s*:\s*ICON_BASE\s*\+\s*"([^"]+)"',
        match.group(1),
    ):
        icon_mapping[key] = filename
    return icon_mapping


def extract_region_configs(source):
    match = re.search(r"const\s+regionConfigs\s*=\s*\[(.*?)\];", source, re.S)
    if not match:
        return []

    region_configs = []
    for name, pattern, icon_key in re.findall(
        r'\{\s*name:\s*"([^"]+)",\s*regex:\s*/(.+?)/,\s*icon:\s*ICON\.([A-Z]+)\s*\}',
        match.group(1),
        re.S,
    ):
        region_configs.append(
            {
                "name": name.strip(),
                "regex": pattern.strip(),
                "icon_key": icon_key.strip(),
            }
        )
    return region_configs


def extract_rule_providers(source):
    match = re.search(r'params\["rule-providers"\]\s*=\s*\{(.*?)\};', source, re.S)
    if not match:
        return []

    providers = []
    for name, provider_type, behavior, filename, path, interval in re.findall(
        r'"([^"]+)"\s*:\s*\{\s*type:\s*"([^"]+)",\s*behavior:\s*"([^"]+)",\s*url:\s*RULE_BASE\s*\+\s*"([^"]+)",\s*path:\s*"([^"]+)",\s*interval:\s*(\d+)\s*\}',
        match.group(1),
        re.S,
    ):
        providers.append(
            {
                "name": name,
                "type": provider_type,
                "behavior": behavior,
                "filename": filename,
                "path": path,
                "interval": int(interval),
            }
        )
    return providers


def extract_dns_config(source):
    match = re.search(r"params\.dns\s*=\s*(\{.*?\})\s*;", source, re.S)
    if not match:
        return {}

    dns_text = re.sub(r",(\s*[}\]])", r"\1", match.group(1))
    try:
        return json.loads(dns_text)
    except json.JSONDecodeError as exc:
        logger.warning("Failed to decode dns config from main.js: %s", exc)
        return {}


def extract_rules_from_js(source):
    match = re.search(r"params\.rules\s*=\s*\[(.*?)\];", source, re.S)
    if not match:
        return []
    return re.findall(r'"((?:[^"\\]|\\.)*)"', match.group(1))


def load_overseer_template():
    template = deepcopy(FALLBACK_OVERSEER_TEMPLATE)
    template["main_js_url"] = MAIN_JS_URL
    template["main_js_fetch_mode"] = "fallback-default"

    logger.info("Syncing rule template from %s", MAIN_JS_URL)
    try:
        response = requests.get(MAIN_JS_URL, timeout=30)
        response.raise_for_status()
        source = response.text

        test_url = extract_js_string_constant(source, "TEST_URL")
        if test_url:
            template["test_url"] = test_url

        test_interval = extract_js_number_constant(source, "TEST_INTERVAL")
        if test_interval is not None:
            template["test_interval"] = test_interval

        rule_base = extract_js_string_constant(source, "RULE_BASE")
        if rule_base:
            template["rule_base"] = rule_base

        icon_base = extract_js_string_constant(source, "ICON_BASE")
        if icon_base:
            template["icon_base"] = icon_base

        icon_mapping = extract_js_icon_mapping(source)
        if icon_mapping:
            template["icons"] = icon_mapping

        region_configs = extract_region_configs(source)
        if region_configs:
            template["region_configs"] = region_configs

        rule_providers = extract_rule_providers(source)
        if rule_providers:
            template["rule_providers"] = rule_providers

        dns_config = extract_dns_config(source)
        if dns_config:
            template["dns"] = dns_config

        rules = extract_rules_from_js(source)
        if rules:
            template["rules"] = rules

        template["main_js_fetch_mode"] = "remote-synced"
        logger.info(
            "Rule template synced: regions=%s, providers=%s, rules=%s, dns=%s",
            len(template["region_configs"]),
            len(template["rule_providers"]),
            len(template["rules"]),
            "yes" if dns_config else "fallback",
        )
    except requests.exceptions.RequestException as exc:
        logger.warning("Failed to fetch main.js, using built-in fallback: %s", exc)
    except Exception as exc:
        logger.warning("Failed to parse main.js, using built-in fallback: %s", exc)

    return template


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


def build_clash_file_text(config, template, results, source_filename, reference_date, target_date):
    generated_at = datetime.now(UTC_PLUS_8).isoformat(timespec="seconds")
    success_sources = [result["source_name"] for result in results if result["proxies"]]
    header = "\n".join(
        [
            f"# generated_at: {generated_at}",
            f"# source_file: {source_filename}",
            f"# reference_date: {reference_date.isoformat()}",
            f"# effective_date: {target_date.isoformat()}",
            f"# rules_source: {template['main_js_url']}",
            f"# rules_mode: {template['main_js_fetch_mode']}",
            f"# success_sources: {', '.join(success_sources) if success_sources else 'none'}",
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
    template = load_overseer_template()

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

    named_proxies, duplicate_name_count = ensure_unique_proxy_names(unique_proxies)
    if duplicate_name_count:
        logger.info("Duplicate proxy names detected and renamed: %s", duplicate_name_count)

    config = build_clash_config(named_proxies, template)
    output_text = build_clash_file_text(
        config,
        template,
        results,
        source_filename,
        reference_date,
        target_date,
    )
    write_text_file(OUTPUT_FILE, output_text)
    logger.info("Clash configuration written to %s", OUTPUT_FILE)


def main():
    args = parse_args()
    target_date = (
        date.fromisoformat(args.target_date)
        if args.target_date
        else datetime.now(UTC_PLUS_8).date()
    )

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

    generate_spider_clash_file(
        results,
        EMBEDDED_SOURCE_LABEL,
        reference_date,
        target_date,
    )

    success_sources = sum(1 for result in results if result["proxies"])
    failed_urls = sum(len(result["errors"]) for result in results)
    logger.info(
        "Task finished: sources=%s, success_sources=%s, failed_urls=%s, output=%s",
        len(results),
        success_sources,
        failed_urls,
        OUTPUT_FILE,
    )


if __name__ == "__main__":
    main()
