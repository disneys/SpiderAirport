import requests
import re
import base64
import os

def process_and_update_link_content_final(markdown_url, output_filename="airport.txt", last_link_filename="last_link.txt", raw_content_filename="airport_base64.yaml"):
    """
    通过 URL 获取 Markdown 文件内容，寻找第一个符合条件的链接，
    如果链接地址与上次不同且成功获取内容，则尝试 Base64 解码后写入 airport.txt，
    并将原始的 Base64 字符串写入 airport_base64.yaml。
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
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
                    latest_content = link_response.text

                    # 保存原始的未解密的 Base64 字符串到 airport_base64.yaml
                    with open(raw_content_filename, "w") as outfile_raw:
                        outfile_raw.write(latest_content + "\n")
                    print(f"原始内容已保存到 {raw_content_filename}")

                    try:
                        # 尝试 Base64 解码并保存到 airport.txt
                        decoded_content = base64.b64decode(latest_content).decode('utf-8')
                        with open(output_filename, "w") as outfile:
                            outfile.write(decoded_content + "\n")
                        print(f"链接内容已获取并 Base64 解码后保存到 {output_filename}")
                        # 更新上次链接
                        with open(last_link_filename, "w") as f_last_link:
                            f_last_link.write(first_link)

                    except base64.binascii.Error:
                        print("链接内容不是 Base64 编码，跳过解码。")
                        # 更新上次链接 (即使解码失败也更新链接，因为链接地址已变化)
                        with open(last_link_filename, "w") as f_last_link:
                            f_last_link.write(first_link)
                    except UnicodeDecodeError:
                        # Base64解码后可能不是UTF-8，尝试 latin-1
                        try:
                            decoded_content = base64.b64decode(latest_content).decode('latin-1')
                            with open(output_filename, "w") as outfile:
                                outfile.write(decoded_content + "\n")
                            print(f"链接内容已获取并 Base64 解码 (latin-1) 后保存到 {output_filename}")
                            # 更新上次链接
                            with open(last_link_filename, "w") as f_last_link:
                                f_last_link.write(first_link)
                        except Exception as e:
                            print(f"获取内容尝试 Base64 解码失败: {e}。")
                            # 更新上次链接 (即使解码失败也更新链接，因为链接地址已变化)
                            with open(last_link_filename, "w") as f_last_link:
                                f_last_link.write(first_link)

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
markdown_url = "https://ghfast.top/https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md"
process_and_update_link_content_final(markdown_url)