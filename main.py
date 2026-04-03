import base64
import logging
import os
import re

import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def write_text_file(file_path, content):
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(content)


def write_error(error_filename, message):
    write_text_file(error_filename, message)
    if message:
        logger.error(message)


def process_and_update_link_content(markdown_url, base_dir, link_pattern):
    """
    通过 URL 获取 Markdown 内容，查找第一个匹配链接。
    每次拿到链接后都重新获取并保存链接内容，避免链接不变但内容已变化。
    如果链接地址变化，则同步更新 last_link.txt。
    """
    headers = {
        "User-Agent": "clash-verge/v2.4.7"
    }
    output_filename = os.path.join(base_dir, "airport.txt")
    last_link_filename = os.path.join(base_dir, "last_link.txt")
    raw_content_filename = os.path.join(base_dir, "airport_base64.txt")
    error_filename = os.path.join(base_dir, "error.txt")

    os.makedirs(base_dir, exist_ok=True)
    write_error(error_filename, "")

    logger.info("开始处理目录: %s", base_dir)
    logger.info("步骤 1/4: 获取 Markdown, url=%s", markdown_url)

    try:
        response = requests.get(markdown_url, timeout=30)
        response.raise_for_status()
        markdown_content = response.text
        logger.info("Markdown 获取成功, 长度=%s", len(markdown_content))
    except requests.exceptions.RequestException as exc:
        write_error(
            error_filename,
            f"获取 Markdown 失败, markdown_url={markdown_url}, error={type(exc).__name__}: {exc}",
        )
        return

    logger.info("步骤 2/4: 在 Markdown 中查找第一个匹配链接")
    links = re.findall(link_pattern, markdown_content)
    if not links:
        write_error(error_filename, f"未找到匹配链接, markdown_url={markdown_url}")
        return

    first_link = links[0]
    logger.info("找到首个匹配链接: %s", first_link)

    previous_link = None
    if os.path.exists(last_link_filename):
        with open(last_link_filename, "r", encoding="utf-8") as f_last_link:
            previous_link = f_last_link.read().strip()

    if first_link != previous_link:
        logger.info("步骤 3/4: 链接已变化，更新 last_link.txt")
        write_text_file(last_link_filename, first_link)
    else:
        logger.info("步骤 3/4: 链接未变化，不更新 last_link.txt")

    logger.info("步骤 4/4: 获取链接内容并刷新 airport_base64.txt")
    try:
        link_response = requests.get(first_link, headers=headers, timeout=30)
        link_response.raise_for_status()
        link_response.encoding = "utf-8"
        response_text = link_response.text.strip()

        write_text_file(raw_content_filename, response_text)
        logger.info(
            "已写入原始内容到 %s, 长度=%s",
            raw_content_filename,
            len(response_text),
        )

        base64_pattern = r"^([A-Za-z0-9+/]{4,}={0,2})$"
        if re.match(base64_pattern, response_text) and len(response_text) > 100:
            logger.info("检测到疑似 Base64 内容，开始解码")
            try:
                decoded_content = base64.b64decode(response_text).decode("utf-8")
                write_text_file(output_filename, decoded_content + "\n")
                logger.info("Base64 已按 utf-8 解码并写入 %s", output_filename)
            except (base64.binascii.Error, UnicodeDecodeError):
                decoded_content = base64.b64decode(response_text).decode("latin-1")
                write_text_file(output_filename, decoded_content + "\n")
                logger.info("Base64 已按 latin-1 解码并写入 %s", output_filename)
        else:
            logger.info("响应内容不是可解码的 Base64，跳过写入 airport.txt")

        write_error(error_filename, "")
        logger.info("处理完成: %s", base_dir)
    except Exception as exc:
        write_error(
            error_filename,
            f"解析链接失败, link={first_link}, error={type(exc).__name__}: {exc}",
        )


if __name__ == "__main__":
    markdown_urls = [
        "https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md",
        "https://raw.githubusercontent.com/mkshare3/mkshare3.github.io/main/README.md",
        "https://raw.githubusercontent.com/abshare/abshare.github.io/main/README.md",
        "https://raw.githubusercontent.com/abshare3/abshare3.github.io/main/README.md",
        "https://raw.githubusercontent.com/tolinkshare2/tolinkshare2.github.io/main/README.md",
        "https://raw.githubusercontent.com/toshare5/toshare5.github.io/main/README.md"
    ]
    url_to_directory = {
        "https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md": "mksshare",
        "https://raw.githubusercontent.com/mkshare3/mkshare3.github.io/main/README.md": "mkshare3",
        "https://raw.githubusercontent.com/abshare/abshare.github.io/main/README.md": "abshare",
        "https://raw.githubusercontent.com/abshare3/abshare3.github.io/main/README.md": "abshare3",
        "https://raw.githubusercontent.com/tolinkshare2/tolinkshare2.github.io/main/README.md": "tolinkshare2",
        "https://raw.githubusercontent.com/toshare5/toshare5.github.io/main/README.md": "toshare5"
    }
    url_to_link_pattern = {
        "https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md": r"https://.*?mcsslk\.xyz/[a-zA-Z0-9]{32}",
        "https://raw.githubusercontent.com/mkshare3/mkshare3.github.io/main/README.md": r"https://.*?mcsslk\.xyz/[a-zA-Z0-9]{32}",
        "https://raw.githubusercontent.com/abshare/abshare.github.io/main/README.md": r"https://.*?absslk\.xyz/[a-zA-Z0-9]{32}",
        "https://raw.githubusercontent.com/abshare3/abshare3.github.io/main/README.md": r"https://.*?absslk\.xyz/[a-zA-Z0-9]{32}",
        "https://raw.githubusercontent.com/tolinkshare2/tolinkshare2.github.io/main/README.md": r"https://.*?tosslk\.xyz/[a-zA-Z0-9]{32}",
        "https://raw.githubusercontent.com/toshare5/toshare5.github.io/main/README.md": r"https://.*?tosslk\.xyz/[a-zA-Z0-9]{32}"
    }

    for url in markdown_urls:
        directory_name = url_to_directory.get(url)
        link_pattern = url_to_link_pattern.get(url)
        if directory_name and link_pattern:
            process_and_update_link_content(url, directory_name, link_pattern)
