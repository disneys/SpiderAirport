import base64
import binascii
import json
import logging
import os
import re
from copy import deepcopy
from datetime import datetime
from urllib.parse import parse_qs, unquote, urlsplit

import requests

try:
    import yaml
except ImportError:
    yaml = None


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


def decode_base64_bytes(value):
    sanitized = re.sub(r"\s+", "", value)
    padded = add_base64_padding(sanitized)
    last_error = None
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            return decoder(padded)
        except (binascii.Error, ValueError) as exc:
            last_error = exc
    raise ValueError(f"Base64 解码失败: {last_error}")


def decode_base64_text(value):
    raw_bytes = decode_base64_bytes(value)
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return raw_bytes.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    raise ValueError("Base64 内容无法按 utf-8 / latin-1 解码")


def is_probably_base64_payload(value):
    sanitized = re.sub(r"\s+", "", value)
    if len(sanitized) < 100:
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9+/=_-]+", sanitized))


def parse_bool(value):
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def parse_csv(value):
    if not value:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def safe_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_query_params(query):
    parsed = parse_qs(query, keep_blank_values=True)
    normalized = {}
    for key, values in parsed.items():
        normalized[key] = values
        normalized.setdefault(key.lower(), values)
    return normalized


def first_query_value(query, *names):
    for name in names:
        values = query.get(name) or query.get(name.lower())
        if values:
            return unquote(values[0])
    return ""


def normalize_proxy_name(name, fallback_name):
    candidate = re.sub(r"\s+", " ", (name or fallback_name or "")).strip()
    return candidate or fallback_name


def split_host_port(value):
    parsed = urlsplit(f"//{value}")
    if not parsed.hostname or parsed.port is None:
        raise ValueError(f"无法解析 host:port -> {value}")
    return parsed.hostname, parsed.port


def parse_ss_plugin(plugin_value):
    if not plugin_value:
        return "", {}
    parts = [unquote(part) for part in plugin_value.split(";") if part]
    if not parts:
        return "", {}
    plugin_name = parts[0]
    plugin_opts = {}
    for item in parts[1:]:
        if "=" in item:
            key, value = item.split("=", 1)
            plugin_opts[key] = value
        else:
            plugin_opts[item] = True
    return plugin_name, plugin_opts


def apply_network_options(proxy, network, query, host_value="", path_value=""):
    network = (network or "").strip().lower()
    if not network or network in {"tcp", "raw"}:
        return

    proxy["network"] = network
    host = host_value or first_query_value(query, "host")
    path = unquote(path_value or first_query_value(query, "path"))

    if network in {"ws", "httpupgrade"}:
        ws_opts = {}
        if path:
            ws_opts["path"] = path
        if host:
            ws_opts["headers"] = {"Host": host}
        if ws_opts:
            proxy["ws-opts"] = ws_opts
        return

    if network == "grpc":
        service_name = first_query_value(query, "serviceName", "grpc-service-name")
        if service_name:
            proxy["grpc-opts"] = {"grpc-service-name": service_name}
        return

    if network == "h2":
        h2_opts = {}
        if host:
            h2_opts["host"] = [item for item in parse_csv(host)]
        if path:
            h2_opts["path"] = path
        if h2_opts:
            proxy["h2-opts"] = h2_opts
        return

    if network == "http":
        http_opts = {"method": "GET"}
        http_path = path or "/"
        http_opts["path"] = [http_path]
        if host:
            http_opts["headers"] = {"Host": parse_csv(host)}
        proxy["http-opts"] = http_opts


def parse_vmess_uri(line, source_name):
    encoded_payload = line[len("vmess://") :]
    decoded_payload, _ = decode_base64_text(encoded_payload)
    data = json.loads(decoded_payload)

    server = data.get("add")
    port = safe_int(data.get("port"))
    uuid = data.get("id")
    if not server or port is None or not uuid:
        raise ValueError("vmess 必要字段缺失")

    proxy = {
        "name": normalize_proxy_name(
            data.get("ps"), f"{source_name}-vmess-{server}:{port}"
        ),
        "type": "vmess",
        "server": server,
        "port": port,
        "uuid": uuid,
        "alterId": safe_int(data.get("aid"), 0),
        "cipher": data.get("scy") or data.get("cipher") or "auto",
        "udp": True,
    }

    security = str(data.get("tls") or data.get("security") or "").strip().lower()
    if security and security != "none":
        proxy["tls"] = True
        server_name = data.get("sni") or data.get("host") or server
        if server_name:
            proxy["servername"] = server_name

    if parse_bool(data.get("allowInsecure")):
        proxy["skip-cert-verify"] = True

    if data.get("fp"):
        proxy["client-fingerprint"] = data["fp"]

    network = data.get("net") or data.get("type")
    apply_network_options(proxy, network, {}, data.get("host", ""), data.get("path", ""))
    return proxy


def parse_ss_uri(line, source_name):
    body = line[len("ss://") :]
    body, _, fragment = body.partition("#")
    body, _, query_string = body.partition("?")
    display_name = unquote(fragment)

    if "@" in body:
        encoded_auth, host_port = body.rsplit("@", 1)
        try:
            decoded_auth, _ = decode_base64_text(encoded_auth)
        except ValueError:
            decoded_auth = encoded_auth
        if ":" not in decoded_auth:
            raise ValueError("ss 用户信息缺少 method:password")
        method, password = decoded_auth.split(":", 1)
    else:
        decoded_body, _ = decode_base64_text(body)
        auth_part, host_port = decoded_body.rsplit("@", 1)
        method, password = auth_part.split(":", 1)

    server, port = split_host_port(host_port)
    query = normalize_query_params(query_string)

    proxy = {
        "name": normalize_proxy_name(display_name, f"{source_name}-ss-{server}:{port}"),
        "type": "ss",
        "server": server,
        "port": port,
        "cipher": method,
        "password": password,
        "udp": True,
    }

    plugin_name, plugin_opts = parse_ss_plugin(first_query_value(query, "plugin"))
    if plugin_name:
        proxy["plugin"] = plugin_name
    if plugin_opts:
        proxy["plugin-opts"] = plugin_opts
    return proxy


def parse_ssr_uri(line, source_name):
    encoded_payload = line[len("ssr://") :]
    decoded_payload, _ = decode_base64_text(encoded_payload)
    main_part, _, query_string = decoded_payload.partition("/?")
    parts = main_part.split(":")
    if len(parts) != 6:
        raise ValueError("ssr 主体格式不正确")

    server, port, protocol, method, obfs, password_b64 = parts
    password, _ = decode_base64_text(password_b64)
    query = normalize_query_params(query_string)

    display_name = ""
    remarks = first_query_value(query, "remarks")
    if remarks:
        try:
            display_name, _ = decode_base64_text(remarks)
        except ValueError:
            display_name = remarks

    proxy = {
        "name": normalize_proxy_name(display_name, f"{source_name}-ssr-{server}:{port}"),
        "type": "ssr",
        "server": server,
        "port": safe_int(port),
        "cipher": method,
        "password": password,
        "protocol": protocol,
        "obfs": obfs,
        "udp": True,
    }

    protocol_param = first_query_value(query, "protoparam")
    obfs_param = first_query_value(query, "obfsparam")
    if protocol_param:
        try:
            protocol_param, _ = decode_base64_text(protocol_param)
        except ValueError:
            pass
        proxy["protocol-param"] = protocol_param
    if obfs_param:
        try:
            obfs_param, _ = decode_base64_text(obfs_param)
        except ValueError:
            pass
        proxy["obfs-param"] = obfs_param
    return proxy


def parse_trojan_uri(line, source_name):
    parsed = urlsplit(line)
    query = normalize_query_params(parsed.query)
    server = parsed.hostname
    port = parsed.port
    password = unquote(parsed.username or "")
    if not server or port is None or not password:
        raise ValueError("trojan 必要字段缺失")

    proxy = {
        "name": normalize_proxy_name(
            unquote(parsed.fragment), f"{source_name}-trojan-{server}:{port}"
        ),
        "type": "trojan",
        "server": server,
        "port": port,
        "password": password,
        "udp": True,
    }

    server_name = first_query_value(query, "sni", "peer", "servername") or server
    if server_name:
        proxy["sni"] = server_name

    if parse_bool(first_query_value(query, "allowInsecure", "insecure")):
        proxy["skip-cert-verify"] = True

    alpn = parse_csv(first_query_value(query, "alpn"))
    if alpn:
        proxy["alpn"] = alpn

    fp = first_query_value(query, "fp")
    if fp:
        proxy["client-fingerprint"] = fp

    network = first_query_value(query, "type", "network")
    apply_network_options(
        proxy,
        network,
        query,
        first_query_value(query, "host"),
        first_query_value(query, "path"),
    )
    return proxy


def parse_vless_uri(line, source_name):
    parsed = urlsplit(line)
    query = normalize_query_params(parsed.query)
    server = parsed.hostname
    port = parsed.port
    uuid = unquote(parsed.username or "")
    if not server or port is None or not uuid:
        raise ValueError("vless 必要字段缺失")

    proxy = {
        "name": normalize_proxy_name(
            unquote(parsed.fragment), f"{source_name}-vless-{server}:{port}"
        ),
        "type": "vless",
        "server": server,
        "port": port,
        "uuid": uuid,
        "udp": True,
    }

    security = first_query_value(query, "security").lower()
    if security in {"tls", "xtls", "reality"}:
        proxy["tls"] = True

    server_name = first_query_value(query, "sni", "peer", "servername") or server
    if proxy.get("tls") and server_name:
        proxy["servername"] = server_name

    if parse_bool(first_query_value(query, "allowInsecure", "insecure")):
        proxy["skip-cert-verify"] = True

    flow = first_query_value(query, "flow")
    if flow:
        proxy["flow"] = flow

    fp = first_query_value(query, "fp")
    if fp:
        proxy["client-fingerprint"] = fp

    if security == "reality":
        reality_opts = {}
        public_key = first_query_value(query, "pbk", "public-key")
        short_id = first_query_value(query, "sid", "short-id")
        if public_key:
            reality_opts["public-key"] = public_key
        if short_id:
            reality_opts["short-id"] = short_id
        if reality_opts:
            proxy["reality-opts"] = reality_opts

    network = first_query_value(query, "type", "network")
    apply_network_options(
        proxy,
        network,
        query,
        first_query_value(query, "host"),
        first_query_value(query, "path"),
    )
    return proxy


def parse_hysteria2_uri(line, source_name):
    parsed = urlsplit(line)
    query = normalize_query_params(parsed.query)
    server = parsed.hostname
    port = parsed.port
    password = unquote(parsed.username or first_query_value(query, "password", "auth"))
    if not server or port is None or not password:
        raise ValueError("hysteria2 必要字段缺失")

    proxy = {
        "name": normalize_proxy_name(
            unquote(parsed.fragment), f"{source_name}-hy2-{server}:{port}"
        ),
        "type": "hysteria2",
        "server": server,
        "port": port,
        "password": password,
        "udp": True,
    }

    sni = first_query_value(query, "sni", "peer")
    if sni:
        proxy["sni"] = sni

    if parse_bool(first_query_value(query, "allowInsecure", "insecure")):
        proxy["skip-cert-verify"] = True

    alpn = parse_csv(first_query_value(query, "alpn"))
    if alpn:
        proxy["alpn"] = alpn

    obfs = first_query_value(query, "obfs")
    obfs_password = first_query_value(query, "obfs-password", "obfsParam")
    if obfs:
        proxy["obfs"] = obfs
    if obfs_password:
        proxy["obfs-password"] = obfs_password

    up_speed = first_query_value(query, "up")
    down_speed = first_query_value(query, "down")
    if up_speed:
        proxy["up"] = up_speed
    if down_speed:
        proxy["down"] = down_speed
    return proxy


def parse_hysteria_uri(line, source_name):
    parsed = urlsplit(line)
    query = normalize_query_params(parsed.query)
    server = parsed.hostname
    port = parsed.port
    auth_value = unquote(parsed.username or first_query_value(query, "auth", "auth-str"))
    if not server or port is None:
        raise ValueError("hysteria 必要字段缺失")

    proxy = {
        "name": normalize_proxy_name(
            unquote(parsed.fragment), f"{source_name}-hy-{server}:{port}"
        ),
        "type": "hysteria",
        "server": server,
        "port": port,
        "udp": True,
    }

    if auth_value:
        proxy["auth-str"] = auth_value

    protocol = first_query_value(query, "protocol")
    if protocol:
        proxy["protocol"] = protocol

    sni = first_query_value(query, "sni", "peer")
    if sni:
        proxy["sni"] = sni

    if parse_bool(first_query_value(query, "allowInsecure", "insecure")):
        proxy["skip-cert-verify"] = True

    up_speed = first_query_value(query, "up")
    down_speed = first_query_value(query, "down")
    if up_speed:
        proxy["up"] = up_speed
    if down_speed:
        proxy["down"] = down_speed

    obfs = first_query_value(query, "obfs")
    if obfs:
        proxy["obfs"] = obfs
    return proxy


def parse_tuic_uri(line, source_name):
    parsed = urlsplit(line)
    query = normalize_query_params(parsed.query)
    server = parsed.hostname
    port = parsed.port
    uuid = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    if not server or port is None or not uuid:
        raise ValueError("tuic 必要字段缺失")

    proxy = {
        "name": normalize_proxy_name(
            unquote(parsed.fragment), f"{source_name}-tuic-{server}:{port}"
        ),
        "type": "tuic",
        "server": server,
        "port": port,
        "uuid": uuid,
        "udp": True,
    }

    if password:
        proxy["password"] = password
    else:
        token = first_query_value(query, "token")
        if token:
            proxy["token"] = token

    sni = first_query_value(query, "sni")
    if sni:
        proxy["sni"] = sni

    if parse_bool(first_query_value(query, "allow_insecure", "allowInsecure")):
        proxy["skip-cert-verify"] = True

    if parse_bool(first_query_value(query, "disable_sni", "disableSni")):
        proxy["disable-sni"] = True

    alpn = parse_csv(first_query_value(query, "alpn"))
    if alpn:
        proxy["alpn"] = alpn

    congestion = first_query_value(
        query, "congestion_control", "congestion-controller"
    )
    if congestion:
        proxy["congestion-controller"] = congestion

    relay_mode = first_query_value(query, "udp_relay_mode", "udp-relay-mode")
    if relay_mode:
        proxy["udp-relay-mode"] = relay_mode

    request_timeout = safe_int(
        first_query_value(query, "request_timeout", "request-timeout")
    )
    heartbeat_interval = safe_int(
        first_query_value(query, "heartbeat_interval", "heartbeat-interval")
    )
    if request_timeout is not None:
        proxy["request-timeout"] = request_timeout
    if heartbeat_interval is not None:
        proxy["heartbeat-interval"] = heartbeat_interval
    return proxy


def parse_proxy_line(line, source_name):
    if line.startswith("vmess://"):
        return parse_vmess_uri(line, source_name)
    if line.startswith("ss://"):
        return parse_ss_uri(line, source_name)
    if line.startswith("ssr://"):
        return parse_ssr_uri(line, source_name)
    if line.startswith("trojan://"):
        return parse_trojan_uri(line, source_name)
    if line.startswith("vless://"):
        return parse_vless_uri(line, source_name)
    if line.startswith("hysteria2://") or line.startswith("hy2://"):
        return parse_hysteria2_uri(line, source_name)
    if line.startswith("hysteria://"):
        return parse_hysteria_uri(line, source_name)
    if line.startswith("tuic://"):
        return parse_tuic_uri(line, source_name)
    return None


def parse_yaml_subscription(decoded_content, source_name):
    if yaml is None:
        return []

    try:
        data = yaml.safe_load(decoded_content)
    except Exception:
        return []

    if not isinstance(data, dict):
        return []

    proxies = data.get("proxies")
    if not isinstance(proxies, list):
        return []

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
    return normalized_proxies


def extract_proxies_from_subscription(decoded_content, source_name):
    yaml_proxies = parse_yaml_subscription(decoded_content, source_name)
    if yaml_proxies:
        logger.info(
            "[%s] 检测到 YAML 订阅，节点数=%s",
            source_name,
            len(yaml_proxies),
        )
        return yaml_proxies

    lines = [
        line.strip()
        for line in decoded_content.replace("\r", "\n").split("\n")
        if line.strip() and not line.strip().startswith("#")
    ]

    proxies = []
    unsupported_count = 0
    failure_samples = []
    for line in lines:
        try:
            proxy = parse_proxy_line(line, source_name)
            if proxy:
                proxies.append(proxy)
            else:
                unsupported_count += 1
        except Exception as exc:
            unsupported_count += 1
            if len(failure_samples) < 3:
                failure_samples.append(f"{line[:80]} -> {type(exc).__name__}: {exc}")

    logger.info(
        "[%s] 节点解析完成，输入行数=%s，成功=%s，跳过/失败=%s",
        source_name,
        len(lines),
        len(proxies),
        unsupported_count,
    )
    if failure_samples:
        logger.warning(
            "[%s] 部分节点未能转换，示例=%s",
            source_name,
            " | ".join(failure_samples),
        )
    return proxies


def uniquify_proxy_names(proxy_entries):
    existing_names = set()
    final_proxies = []

    for entry in proxy_entries:
        source_name = entry["source_name"]
        proxy = deepcopy(entry["proxy"])
        base_name = normalize_proxy_name(
            proxy.get("name"),
            f"{source_name}-{proxy.get('type', 'proxy')}-{proxy.get('server', 'unknown')}",
        )
        candidate = base_name
        if candidate in existing_names:
            candidate = f"{base_name} [{source_name}]"

        index = 2
        while candidate in existing_names:
            candidate = f"{base_name} [{source_name}-{index}]"
            index += 1

        proxy["name"] = candidate
        existing_names.add(candidate)
        final_proxies.append(proxy)
    return final_proxies


def build_icon_url(template, icon_key):
    icon_name = template["icons"].get(icon_key)
    if not icon_name:
        return ""
    return f"{template['icon_base']}{icon_name}"


def extract_js_string_constant(source, constant_name):
    pattern = re.compile(rf'const\s+{constant_name}\s*=\s*"([^"]+)"')
    match = pattern.search(source)
    return match.group(1) if match else ""


def extract_js_number_constant(source, constant_name):
    pattern = re.compile(rf"const\s+{constant_name}\s*=\s*(\d+)")
    match = pattern.search(source)
    return safe_int(match.group(1)) if match else None


def extract_js_icon_mapping(source):
    icon_block_match = re.search(r"const\s+ICON\s*=\s*\{(.*?)\};", source, re.S)
    if not icon_block_match:
        return {}

    icon_mapping = {}
    for key, filename in re.findall(
        r'([A-Z]+)\s*:\s*ICON_BASE\s*\+\s*"([^"]+)"',
        icon_block_match.group(1),
    ):
        icon_mapping[key] = filename
    return icon_mapping


def extract_region_configs(source):
    region_block_match = re.search(
        r"const\s+regionConfigs\s*=\s*\[(.*?)\];", source, re.S
    )
    if not region_block_match:
        return []

    region_configs = []
    for name, pattern, icon_key in re.findall(
        r'\{\s*name:\s*"([^"]+)",\s*regex:\s*/(.+?)/,\s*icon:\s*ICON\.([A-Z]+)\s*\}',
        region_block_match.group(1),
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
    provider_block_match = re.search(
        r'params\["rule-providers"\]\s*=\s*\{(.*?)\};', source, re.S
    )
    if not provider_block_match:
        return []

    providers = []
    for name, provider_type, behavior, filename, path, interval in re.findall(
        r'"([^"]+)"\s*:\s*\{\s*type:\s*"([^"]+)",\s*behavior:\s*"([^"]+)",\s*url:\s*RULE_BASE\s*\+\s*"([^"]+)",\s*path:\s*"([^"]+)",\s*interval:\s*(\d+)\s*\}',
        provider_block_match.group(1),
        re.S,
    ):
        providers.append(
            {
                "name": name,
                "type": provider_type,
                "behavior": behavior,
                "filename": filename,
                "path": path,
                "interval": safe_int(interval, 86400),
            }
        )
    return providers


def extract_rules_from_js(source):
    rules_match = re.search(r"params\.rules\s*=\s*\[(.*?)\];", source, re.S)
    if not rules_match:
        return []
    return re.findall(r'"((?:[^"\\]|\\.)*)"', rules_match.group(1))


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
            "联动规则模板同步成功: 分组=%s, 规则源=%s, 规则条数=%s",
            len(template["region_configs"]),
            len(template["rule_providers"]),
            len(template["rules"]),
        )
    except requests.exceptions.RequestException as exc:
        logger.warning(
            "main.js 拉取失败，使用内置模板继续生成 airport_clash.txt: %s",
            exc,
        )
    except Exception as exc:
        logger.warning(
            "main.js 解析失败，使用内置模板继续生成 airport_clash.txt: %s",
            exc,
        )
    return template


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
            logger.warning("区域正则无效，已跳过 %s: %s", region["name"], exc)
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


def clean_data(value):
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if key.startswith("_"):
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


def build_clash_config(all_proxies, template):
    proxy_names = [proxy["name"] for proxy in all_proxies]
    config = {
        "mixed-port": 7890,
        "allow-lan": False,
        "mode": "rule",
        "log-level": "info",
        "ipv6": False,
        "unified-delay": True,
        "tcp-concurrent": True,
        "global-client-fingerprint": "chrome",
        "profile": {
            "store-selected": True,
            "store-fake-ip": True,
        },
        "dns": deepcopy(template["dns"]),
        "proxies": all_proxies,
        "proxy-groups": build_proxy_groups(proxy_names, template),
        "rule-providers": build_rule_providers(template),
        "rules": template["rules"],
    }
    return clean_data(config)


def yaml_scalar(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(str(value), ensure_ascii=False)


def yaml_key(value):
    key = str(value)
    if re.fullmatch(r"[A-Za-z0-9_-]+", key):
        return key
    return json.dumps(key, ensure_ascii=False)


def dump_yaml(value, indent=0):
    spaces = " " * indent

    if isinstance(value, dict):
        if not value:
            return f"{spaces}{{}}"
        lines = []
        for key, item in value.items():
            if isinstance(item, (dict, list)):
                if not item:
                    empty_repr = "{}" if isinstance(item, dict) else "[]"
                    lines.append(f"{spaces}{yaml_key(key)}: {empty_repr}")
                else:
                    lines.append(f"{spaces}{yaml_key(key)}:")
                    lines.append(dump_yaml(item, indent + 2))
            else:
                lines.append(f"{spaces}{yaml_key(key)}: {yaml_scalar(item)}")
        return "\n".join(lines)


    if isinstance(value, list):
        if not value:
            return f"{spaces}[]"
        lines = []
        for item in value:
            if isinstance(item, dict):
                if not item:
                    lines.append(f"{spaces}- {{}}")
                    continue
                first = True
                for key, sub_item in item.items():
                    if first:
                        if isinstance(sub_item, (dict, list)) and sub_item:
                            lines.append(f"{spaces}- {yaml_key(key)}:")
                            lines.append(dump_yaml(sub_item, indent + 4))
                        elif isinstance(sub_item, (dict, list)):
                            empty_repr = "{}" if isinstance(sub_item, dict) else "[]"
                            lines.append(f"{spaces}- {yaml_key(key)}: {empty_repr}")
                        else:
                            lines.append(
                                f"{spaces}- {yaml_key(key)}: {yaml_scalar(sub_item)}"
                            )
                        first = False
                    else:
                        if isinstance(sub_item, (dict, list)) and sub_item:
                            lines.append(f"{spaces}  {yaml_key(key)}:")
                            lines.append(dump_yaml(sub_item, indent + 4))
                        elif isinstance(sub_item, (dict, list)):
                            empty_repr = "{}" if isinstance(sub_item, dict) else "[]"
                            lines.append(f"{spaces}  {yaml_key(key)}: {empty_repr}")
                        else:
                            lines.append(
                                f"{spaces}  {yaml_key(key)}: {yaml_scalar(sub_item)}"
                            )
            elif isinstance(item, list):
                if item:
                    lines.append(f"{spaces}-")
                    lines.append(dump_yaml(item, indent + 2))
                else:
                    lines.append(f"{spaces}- []")
            else:
                lines.append(f"{spaces}- {yaml_scalar(item)}")
        return "\n".join(lines)

    return f"{spaces}{yaml_scalar(value)}"


def build_clash_file_text(config, template, results):
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    success_sources = [result["source_name"] for result in results if result["proxies"]]
    header_lines = [
        "# 自动生成，请勿手动编辑",
        f"# generated_at: {generated_at}",
        f"# rules_source: {template['main_js_url']}",
        f"# rules_mode: {template['main_js_fetch_mode']}",
        f"# success_sources: {', '.join(success_sources) if success_sources else 'none'}",
        "",
    ]
    return "\n".join(header_lines) + dump_yaml(config) + "\n"


def generate_airport_clash_file(results):
    logger.info("开始汇总 airport_clash.txt")
    template = load_overseer_template()

    proxy_entries = []
    for result in results:
        if not result["proxies"]:
            continue
        for proxy in result["proxies"]:
            proxy_entries.append(
                {
                    "source_name": result["source_name"],
                    "proxy": proxy,
                }
            )

    all_proxies = uniquify_proxy_names(proxy_entries)
    logger.info(
        "订阅汇总完成: 有效来源=%s, 节点总数=%s",
        len({entry['source_name'] for entry in proxy_entries}),
        len(all_proxies),
    )
    if not all_proxies:
        logger.warning("本次未解析到任何节点，将生成 DIRECT 回退版 airport_clash.txt")

    config = build_clash_config(all_proxies, template)
    airport_clash_path = os.path.join(ROOT_DIR, "airport_clash.txt")
    write_text_file(airport_clash_path, build_clash_file_text(config, template, results))
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
        logger.info(
            "[%s] airport_base64.txt 已刷新，长度=%s",
            source_name,
            len(raw_content),
        )
    except Exception as exc:
        write_error(
            error_filename,
            f"拉取订阅原文失败，link={first_link}，error={type(exc).__name__}: {exc}",
        )
        return result

    log_step(source_name, 5, total_steps, "解码订阅内容并刷新 airport.txt")
    try:
        if is_probably_base64_payload(raw_content):
            decoded_content, decoded_encoding = decode_base64_text(raw_content)
            decode_mode = f"base64/{decoded_encoding}"
        else:
            decoded_content = raw_content
            decode_mode = "plain-text"

        decoded_content = normalize_text(decoded_content)
        if not decoded_content:
            raise ValueError("订阅解码后为空")

        write_text_file(output_filename, decoded_content + "\n")
        logger.info(
            "[%s] airport.txt 已刷新，解码模式=%s，长度=%s",
            source_name,
            decode_mode,
            len(decoded_content),
        )
    except Exception as exc:
        write_error(
            error_filename,
            f"解码订阅失败，link={first_link}，error={type(exc).__name__}: {exc}",
        )
        return result

    log_step(source_name, 6, total_steps, "解析节点并参与汇总生成 airport_clash.txt")
    try:
        proxies = extract_proxies_from_subscription(decoded_content, source_name)
        if not proxies:
            raise ValueError("未能从订阅内容中解析出任何支持的节点")

        result["proxies"] = proxies
        write_error(error_filename, "")
        logger.info(
            "[%s] 全流程成功，节点数=%s，error.txt 已清空",
            source_name,
            len(proxies),
        )
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
