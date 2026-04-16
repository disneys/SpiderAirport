import base64
import binascii
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import requests
import yaml


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SUBSCRIPTION_HEADERS = {"User-Agent": "clash-verge/v2.4.7"}
MAIN_JS_URL = (
    "https://raw.githubusercontent.com/disneys/"
    "Mihomo-Dynamic-Overseer/refs/heads/main/main.js"
)
UTC_PLUS_8 = timezone(timedelta(hours=8))

DEFAULT_OVERSEER_TEMPLATE = {
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
        {"name": "香港", "regex": r"港|香港|🇭🇰|HK|Hong Kong", "icon_key": "HK"},
        {"name": "台湾", "regex": r"台|台湾|新北|彰化|TW|Taiwan|🇹🇼", "icon_key": "TW"},
        {"name": "新加坡", "regex": r"新加坡|狮城|SG|Singapore|🇸🇬", "icon_key": "SG"},
        {"name": "日本", "regex": r"日本|🇯🇵|JP|Japan", "icon_key": "JP"},
        {"name": "美国", "regex": r"美|美国|🇺🇸|US|United States", "icon_key": "US"},
        {"name": "韩国", "regex": r"韩|韩国|🇰🇷|KR|Korea", "icon_key": "KR"},
        {"name": "英国", "regex": r"英国|🇬🇧|UK|United Kingdom", "icon_key": "UK"},
        {"name": "德国", "regex": r"德|德国|🇩🇪|DE|Germany", "icon_key": "DE"},
        {"name": "法国", "regex": r"法|法国|🇫🇷|FR|France", "icon_key": "FR"},
        {"name": "加拿大", "regex": r"加拿大|🇨🇦|CA|Canada", "icon_key": "CA"},
        {"name": "澳洲", "regex": r"澳|澳大利亚|🇦🇺|AU|Australia", "icon_key": "AU"},
        {"name": "俄罗斯", "regex": r"俄|俄罗斯|🇷🇺|RU|Russia", "icon_key": "RU"},
        {"name": "印度", "regex": r"印|印度|🇮🇳|IN|India", "icon_key": "IN"},
        {"name": "荷兰", "regex": r"荷|荷兰|🇳🇱|NL|Netherlands", "icon_key": "NL"},
        {"name": "土耳其", "regex": r"土|土耳其|🇹🇷|TR|Turkey", "icon_key": "TR"},
        {"name": "巴西", "regex": r"巴西|🇧🇷|BR|Brazil", "icon_key": "BR"},
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


def write_text_file(file_path, content):
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(content)


def write_error(error_filename, message):
    write_text_file(error_filename, message)
    if message:
        logger.error(message)


def log_step(source_name, step, total_steps, message):
    logger.info("[%s] 步骤 %s/%s: %s", source_name, step, total_steps, message)


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

    raise ValueError("Base64 内容无法解码为文本")


def normalize_proxy_name(name, fallback_name):
    candidate = re.sub(r"\s+", " ", str(name or fallback_name or "")).strip()
    return candidate or fallback_name


def extract_yaml_proxies(content, source_name):
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        raise ValueError(f"PyYAML 解析失败: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError("订阅内容不是 YAML 字典")

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
        normalized_proxies.append(cloned)

    if not normalized_proxies:
        raise ValueError("proxies 列表为空或格式不正确")

    return normalized_proxies


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


def fetch_remote_main_js_source():
    response = requests.get(MAIN_JS_URL, timeout=30)
    response.raise_for_status()
    return response.text


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
            raise RuntimeError(
                f"node failed with exit code {completed.returncode}: {completed.stderr.strip()}"
            )

        stdout = completed.stdout.strip()
        if not stdout:
            raise RuntimeError("main.js returned empty output")

        result = json.loads(stdout)
        if not isinstance(result, dict):
            raise ValueError("main.js result is not a JSON object")

        required_keys = ["dns", "proxy-groups", "rule-providers", "rules"]
        missing_keys = [key for key in required_keys if key not in result]
        if missing_keys:
            raise ValueError(
                f"main.js result is missing required keys: {', '.join(missing_keys)}"
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
                "Self-referencing proxy groups detected but conflicting proxies "
                f"could not be renamed: {', '.join(loop_names)}"
            )

        total_renamed += renamed_count
        current_proxies = renamed_proxies
        logger.warning(
            "Remote main.js produced self-referencing proxy groups; renamed %s nodes, groups=%s, attempt=%s",
            renamed_count,
            ", ".join(loop_names),
            attempt,
        )

    raise ValueError(
        "Self-referencing proxy groups still exist after automatic renaming"
    )


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


def extract_rules_from_js(source):
    match = re.search(r"params\.rules\s*=\s*\[(.*?)\];", source, re.S)
    if not match:
        return []
    return re.findall(r'"((?:[^"\\]|\\.)*)"', match.group(1))


def load_overseer_template():
    template = deepcopy(DEFAULT_OVERSEER_TEMPLATE)
    template["main_js_url"] = MAIN_JS_URL
    template["main_js_fetch_mode"] = "embedded-default"

    logger.info("开始同步联动规则模板: %s", MAIN_JS_URL)
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

        rules = extract_rules_from_js(source)
        if rules:
            template["rules"] = rules

        template["main_js_fetch_mode"] = "remote-synced"
        logger.info(
            "联动规则模板同步成功: 分组=%s, 规则源=%s, 规则数=%s",
            len(template["region_configs"]),
            len(template["rule_providers"]),
            len(template["rules"]),
        )
    except requests.exceptions.RequestException as exc:
        logger.warning("main.js 拉取失败，使用内置模板继续生成: %s", exc)
    except Exception as exc:
        logger.warning("main.js 解析失败，使用内置模板继续生成: %s", exc)

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
            logger.warning("区域正则无效，跳过 %s: %s", region["name"], exc)
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
        if not re.search(r"港|香港|HK|CN|中国|Direct", name, re.I)
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
                "Fallback config has self-referencing proxy groups but conflicting "
                f"proxies could not be renamed: {', '.join(loop_names)}"
            )

        total_renamed += renamed_count
        current_proxies = renamed_proxies
        logger.warning(
            "Fallback config produced self-referencing proxy groups; renamed %s nodes, groups=%s, attempt=%s",
            renamed_count,
            ", ".join(loop_names),
            attempt,
        )

    raise ValueError(
        "Fallback proxy groups still self-reference after automatic renaming"
    )


def build_generation_metadata():
    generated_at = datetime.now(UTC_PLUS_8).replace(microsecond=0)
    marker_prefix = "\u23f0\u914d\u7f6e\u751f\u6210\u4e8e\uff1a"
    display_time = generated_at.strftime("%Y-%m-%d %H:%M:%S").replace(":", "\uff1a")
    marker_name = f"{marker_prefix}{display_time}"
    return generated_at.isoformat(), marker_name


def add_generation_marker_proxy_group(config, marker_name):
    marker_prefix = "\u23f0\u914d\u7f6e\u751f\u6210\u4e8e\uff1a"
    marker_group = {
        "name": marker_name,
        "type": "select",
        "proxies": ["DIRECT"],
    }
    proxy_groups = [
        group
        for group in config.get("proxy-groups", [])
        if not (
            isinstance(group, dict)
            and str(group.get("name", "")).startswith(marker_prefix)
        )
    ]
    config["proxy-groups"] = [marker_group, *proxy_groups]
    return config


def build_clash_file_text(
    config,
    results,
    rules_source,
    rules_mode,
    generated_at,
    generation_marker_name,
):
    success_sources = [result["source_name"] for result in results if result["proxies"]]
    header = "\n".join(
        [
            f"# {generation_marker_name}",
            f"# generated_at: {generated_at}",
            f"# rules_source: {rules_source}",
            f"# rules_mode: {rules_mode}",
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


def generate_airport_clash_file(results):
    logger.info("开始汇总 airport_clash.txt")
    generated_at, generation_marker_name = build_generation_metadata()
    collected_proxies = []
    for result in results:
        collected_proxies.extend(result["proxies"])

    unique_proxies = deduplicate_proxies(collected_proxies)
    logger.info(
        "节点汇总完成: 原始=%s, 去重后=%s",
        len(collected_proxies),
        len(unique_proxies),
    )
    if not unique_proxies:
        logger.warning("本次未提取到任何节点，将生成 DIRECT 回退配置")

    named_proxies, duplicate_name_count = ensure_unique_proxy_names(unique_proxies)
    if duplicate_name_count:
        logger.info("检测到重复节点名，已自动重命名数量=%s", duplicate_name_count)

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

    config = add_generation_marker_proxy_group(config, generation_marker_name)
    airport_clash_path = os.path.join(ROOT_DIR, "airport_clash.txt")
    write_text_file(
        airport_clash_path,
        build_clash_file_text(
            config,
            results,
            rules_source,
            rules_mode,
            generated_at,
            generation_marker_name,
        ),
    )
    logger.info("已写入 Clash/Mihomo 配置: %s", airport_clash_path)


def process_and_update_link_content(markdown_url, base_dir, link_pattern):
    source_name = base_dir
    output_filename = os.path.join(base_dir, "airport.txt")
    last_link_filename = os.path.join(base_dir, "last_link.txt")
    raw_content_filename = os.path.join(base_dir, "airport_base64.txt")
    error_filename = os.path.join(base_dir, "error.txt")
    total_steps = 6

    os.makedirs(base_dir, exist_ok=True)
    write_error(error_filename, "")

    logger.info("[%s] 开始处理，markdown=%s", source_name, markdown_url)
    result = {
        "source_name": source_name,
        "markdown_url": markdown_url,
        "subscription_url": "",
        "proxies": [],
    }

    log_step(source_name, 1, total_steps, "拉取 Markdown 内容")
    try:
        response = requests.get(markdown_url, timeout=30)
        response.raise_for_status()
        markdown_content = response.text
        logger.info("[%s] Markdown 拉取成功，长度=%s", source_name, len(markdown_content))
    except requests.exceptions.RequestException as exc:
        write_error(
            error_filename,
            f"拉取 Markdown 失败，markdown_url={markdown_url}，error={type(exc).__name__}: {exc}",
        )
        return result

    log_step(source_name, 2, total_steps, "匹配首个订阅链接")
    links = re.findall(link_pattern, markdown_content)
    if not links:
        write_error(
            error_filename,
            f"未找到匹配链接，markdown_url={markdown_url}，pattern={link_pattern}",
        )
        return result

    first_link = links[0]
    result["subscription_url"] = first_link
    logger.info("[%s] 已找到订阅链接: %s", source_name, first_link)

    previous_link = ""
    if os.path.exists(last_link_filename):
        with open(last_link_filename, "r", encoding="utf-8") as file:
            previous_link = file.read().strip()

    log_step(source_name, 3, total_steps, "同步 last_link.txt")
    if first_link != previous_link:
        write_text_file(last_link_filename, first_link)
        logger.info("[%s] 订阅链接已变化，last_link.txt 已更新", source_name)
    else:
        logger.info("[%s] 订阅链接未变化，last_link.txt 保持不变", source_name)

    log_step(source_name, 4, total_steps, "拉取订阅原文并刷新 airport_base64.txt")
    try:
        link_response = requests.get(first_link, headers=SUBSCRIPTION_HEADERS, timeout=30)
        link_response.raise_for_status()
        link_response.encoding = "utf-8"
        raw_content = normalize_text(link_response.text)
        if not raw_content:
            raise ValueError("订阅响应为空")

        write_text_file(raw_content_filename, raw_content + "\n")
        logger.info("[%s] airport_base64.txt 已刷新，长度=%s", source_name, len(raw_content))
    except Exception as exc:
        write_error(
            error_filename,
            f"拉取订阅原文失败，link={first_link}，error={type(exc).__name__}: {exc}",
        )
        return result

    log_step(source_name, 5, total_steps, "刷新 airport.txt")
    try:
        if is_probably_base64_payload(raw_content):
            decoded_content, decode_encoding = decode_base64_text(raw_content)
            decode_mode = f"base64/{decode_encoding}"
        else:
            decoded_content = raw_content
            decode_mode = "plain-text"

        decoded_content = normalize_text(decoded_content)
        write_text_file(output_filename, decoded_content + "\n")
        logger.info("[%s] airport.txt 已刷新，模式=%s，长度=%s", source_name, decode_mode, len(decoded_content))
    except Exception as exc:
        write_error(
            error_filename,
            f"刷新 airport.txt 失败，link={first_link}，error={type(exc).__name__}: {exc}",
        )
        return result

    log_step(source_name, 6, total_steps, "从 airport_base64 的 YAML proxies 提取节点")
    try:
        proxies = extract_yaml_proxies(raw_content, source_name)
        result["proxies"] = proxies
        write_error(error_filename, "")
        logger.info("[%s] 节点提取成功，节点数=%s，error.txt 已清空", source_name, len(proxies))
    except Exception as exc:
        write_error(
            error_filename,
            f"解析节点失败，link={first_link}，error={type(exc).__name__}: {exc}",
        )

    return result


def main():
    markdown_urls = [
        "https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md",
        "https://raw.githubusercontent.com/mkshare3/mkshare3.github.io/main/README.md",
        "https://raw.githubusercontent.com/abshare/abshare.github.io/main/README.md",
        "https://raw.githubusercontent.com/abshare3/abshare3.github.io/main/README.md",
        "https://raw.githubusercontent.com/tolinkshare2/tolinkshare2.github.io/main/README.md",
        "https://raw.githubusercontent.com/toshare5/toshare5.github.io/main/README.md",
    ]
    url_to_directory = {
        "https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md": "mksshare",
        "https://raw.githubusercontent.com/mkshare3/mkshare3.github.io/main/README.md": "mkshare3",
        "https://raw.githubusercontent.com/abshare/abshare.github.io/main/README.md": "abshare",
        "https://raw.githubusercontent.com/abshare3/abshare3.github.io/main/README.md": "abshare3",
        "https://raw.githubusercontent.com/tolinkshare2/tolinkshare2.github.io/main/README.md": "tolinkshare2",
        "https://raw.githubusercontent.com/toshare5/toshare5.github.io/main/README.md": "toshare5",
    }
    url_to_link_pattern = {
        "https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md": r"https://.*?mcsslk\.xyz/[a-zA-Z0-9]{32}",
        "https://raw.githubusercontent.com/mkshare3/mkshare3.github.io/main/README.md": r"https://.*?mcsslk\.xyz/[a-zA-Z0-9]{32}",
        "https://raw.githubusercontent.com/abshare/abshare.github.io/main/README.md": r"https://.*?absslk\.xyz/[a-zA-Z0-9]{32}",
        "https://raw.githubusercontent.com/abshare3/abshare3.github.io/main/README.md": r"https://.*?absslk\.xyz/[a-zA-Z0-9]{32}",
        "https://raw.githubusercontent.com/tolinkshare2/tolinkshare2.github.io/main/README.md": r"https://.*?tosslk\.xyz/[a-zA-Z0-9]{32}",
        "https://raw.githubusercontent.com/toshare5/toshare5.github.io/main/README.md": r"https://.*?tosslk\.xyz/[a-zA-Z0-9]{32}",
    }

    logger.info("任务开始：抓取订阅、刷新 airport_base64.txt、生成 airport_clash.txt")
    results = []
    for url in markdown_urls:
        directory_name = url_to_directory.get(url)
        link_pattern = url_to_link_pattern.get(url)
        if not directory_name or not link_pattern:
            logger.warning("跳过未配置来源: %s", url)
            continue
        results.append(process_and_update_link_content(url, directory_name, link_pattern))

    generate_airport_clash_file(results)
    success_count = sum(1 for result in results if result["proxies"])
    logger.info(
        "任务结束：来源总数=%s，成功来源=%s，根目录配置=%s",
        len(results),
        success_count,
        os.path.join(ROOT_DIR, "airport_clash.txt"),
    )


if __name__ == "__main__":
    main()
