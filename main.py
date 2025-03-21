import requests
import re
import base64
import os
from bs4 import BeautifulSoup

def process_and_update_link_content_final(markdown_url, output_filename="airport.txt", last_link_filename="last_link.txt", raw_content_filename="airport_base64.txt"):
    """
    通过 URL 获取 Markdown 文件内容，寻找第一个符合条件的链接，
    如果链接地址与上次不同且成功获取内容，则模拟 Chrome 行为保存完整内容，并搜索 Base64 解码。
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
            print(f"找到链接: {first_link}")

            previous_link = None
            if os.path.exists(last_link_filename):
                with open(last_link_filename, "r") as f_last_link:
                    previous_link = f_last_link.read().strip()

            if first_link != previous_link:
                print("链接地址已更改，正在获取内容并更新。")
                try:
                    link_response = requests.get(first_link, headers=headers)
                    link_response.raise_for_status()
                    link_response.encoding = 'utf-8'  # 强制指定编码为 UTF-8 (放在获取文本内容之前)
                    html_content = link_response.text

                    # 保存完整的 HTML 内容到 airport_base64.txt
                    with open(raw_content_filename, "w", encoding='utf-8') as outfile_raw:
                        outfile_raw.write(html_content)
                    print(f"原始链接的完整内容已保存到 {raw_content_filename}")

                    # 使用正则表达式搜索 HTML 内容中的 Base64 编码字符串
                    base64_pattern = r"([a-zA-Z0-9+/]{4,}={0,2})"
                    base64_matches = re.findall(base64_pattern, html_content)

                    if base64_matches:
                        # 假设第一个匹配到的长字符串是配置信息
                        likely_base64 = max(base64_matches, key=len) # 找到最长的匹配项
                        print(f"找到疑似 Base64 编码的字符串: {likely_base64[:50]}...") # 打印前 50 个字符

                        try:
                            decoded_content = base64.b64decode(likely_base64).decode('utf-8')
                            with open(output_filename, "w", encoding='utf-8') as outfile:
                                outfile.write(decoded_content + "\n")
                            print(f"提取到的 Base64 内容已解码并保存到 {output_filename}")
                            # 更新上次链接
                            with open(last_link_filename, "w", encoding='utf-8') as f_last_link:
                                f_last_link.write(first_link)

                        except base64.binascii.Error:
                            print("提取到的内容不是有效的 Base64 编码，跳过解码。")
                            with open(last_link_filename, "w", encoding='utf-8') as f_last_link:
                                f_last_link.write(first_link)
                        except UnicodeDecodeError:
                            try:
                                decoded_content = base64.b64decode(likely_base64).decode('latin-1')
                                with open(output_filename, "w", encoding='utf-8') as outfile:
                                    outfile.write(decoded_content + "\n")
                                print(f"提取到的 Base64 内容已解码 (latin-1) 并保存到 {output_filename}")
                                with open(last_link_filename, "w", encoding='utf-8') as f_last_link:
                                    f_last_link.write(first_link)
                            except Exception as e:
                                print(f"提取到的 Base64 内容尝试解码失败: {e}。")
                                with open(last_link_filename, "w", encoding='utf-8') as f_last_link:
                                    f_last_link.write(first_link)
                    else:
                        print("在 HTML 内容中未找到疑似 Base64 编码的字符串。")

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 403:
                        print(f"获取链接内容时发生错误: 403 Forbidden。跳过更新。")
                    else:
                        print(f"获取链接内容时发生 HTTP 错误: {e}。")
                except requests.exceptions.RequestException as e:
                    print(f"获取链接内容时发生其他错误: {e}")

            else:
                print("链接地址与上次相同，无需更新内容。")

        else:
            print(f"在 URL: {markdown_url} 的 Markdown 文件中没有找到符合条件的链接。")

    except requests.exceptions.RequestException as e:
        print(f"获取 Markdown 文件内容时发生错误: {e}")

# 示例用法
markdown_url = "https://wget.la/https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md"
process_and_update_link_content_final(markdown_url)