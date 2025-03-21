import requests
import re
import base64
import os

def process_and_update_link_content_final(markdown_url="https://wget.la/https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md", output_filename="airport.txt", last_link_filename="last_link.txt", raw_content_filename="airport_base64.txt"):
    """
    通过 URL 获取 Markdown 文件内容，寻找第一个符合条件的链接，
    如果链接地址与上次不同且成功获取内容，则尝试将响应文本直接作为 Base64 解码。
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(markdown_url)
        response.raise_for_status()
        markdown_content = response.text

        pattern = r"https://.*?mcsslk\.xyz/[a-zA-Z0-9]{32}"
        links = re.findall(pattern, markdown_content)

        if links:
            first_link = links[0]
            # print(f"找到链接: {first_link}")

            previous_link = None
            if os.path.exists(last_link_filename):
                with open(last_link_filename, "r") as f_last_link:
                    previous_link = f_last_link.read().strip()

            if first_link != previous_link:
                # print("链接地址已更改，正在获取内容并更新。")
                try:
                    link_response = requests.get(first_link, headers=headers)
                    link_response.raise_for_status()
                    link_response.encoding = 'utf-8'
                    response_text = link_response.text.strip()

                    with open(raw_content_filename, "w", encoding='utf-8') as outfile_raw:
                        outfile_raw.write(response_text)
                    # print(f"原始链接的响应文本已保存到 {raw_content_filename}")

                    base64_pattern = r"^([A-Za-z0-9+/]{4,}={0,2})$"
                    if re.match(base64_pattern, response_text) and len(response_text) > 100:
                        # print(f"响应文本看起来像是 Base64 编码: {response_text[:50]}...")
                        try:
                            decoded_content = base64.b64decode(response_text).decode('utf-8')
                            with open(output_filename, "w", encoding='utf-8') as outfile:
                                outfile.write(decoded_content + "\n")
                            # print(f"响应文本已解码并保存到 {output_filename}")
                            with open(last_link_filename, "w", encoding='utf-8') as f_last_link:
                                f_last_link.write(first_link)

                        except (base64.binascii.Error, UnicodeDecodeError):
                            try:
                                decoded_content = base64.b64decode(response_text).decode('latin-1')
                                with open(output_filename, "w", encoding='utf-8') as outfile:
                                    outfile.write(decoded_content + "\n")
                                # print(f"响应文本已解码 (latin-1) 并保存到 {output_filename}")
                                with open(last_link_filename, "w", encoding='utf-8') as f_last_link:
                                    f_last_link.write(first_link)
                            except Exception:
                                with open(last_link_filename, "w", encoding='utf-8') as f_last_link:
                                    f_last_link.write(first_link)
                    else:
                        # print("响应文本看起来不像是 Base64 编码，跳过解码。")
                        with open(last_link_filename, "w", encoding='utf-8') as f_last_link:
                            f_last_link.write(first_link)

                except requests.exceptions.RequestException:
                    pass # print(f"获取链接内容时发生错误: {e}")
            else:
                pass # print("链接地址与上次相同，无需更新内容。")
        else:
            pass # print(f"在 URL: {markdown_url} 的 Markdown 文件中没有找到符合条件的链接。")
    except requests.exceptions.RequestException:
        pass # print(f"获取 Markdown 文件内容时发生错误: {e}")

# 示例用法
# markdown_url = "https://ghfast.top/https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md"
# process_and_update_link_content_final(markdown_url)
process_and_update_link_content_final()