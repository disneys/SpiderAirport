import requests
import re
import base64

def process_and_write_first_link_content(markdown_url, output_filename="airport.txt"):
    """
    通过 URL 获取 Markdown 文件内容，寻找第一个以 https:// 开头、包含 "mcsslk.xyz" 且后跟 32 位字符串的链接，
    访问该链接获取内容，尝试 Base64 解码，并将解码后的内容写入到指定的文件中（不包含链接信息）。

    Args:
        markdown_url (str): 包含链接的 Markdown 文件的 URL。
        output_filename (str): 输出文件名，默认为 "airport.txt"。
    """
    found_links = set()  # 用于存储已经找到的链接，确保只处理第一个
    link_found_and_processed = False

    try:
        response = requests.get(markdown_url)
        response.raise_for_status()
        markdown_content = response.text

        # 使用正则表达式查找以 https:// 开头、包含 "mcsslk.xyz" 且后跟 32 位字母数字字符串的链接
        pattern = r"https://.*?mcsslk\.xyz/[a-zA-Z0-9]{32}"
        links = re.findall(pattern, markdown_content)

        if links:
            with open(output_filename, "w") as outfile:
                for link in links:
                    if link not in found_links:
                        print(f"正在处理链接: {link}")
                        found_links.add(link)
                        link_found_and_processed = True  # 标记已找到并处理了链接

                        try:
                            link_response = requests.get(link)
                            link_response.raise_for_status()
                            link_content = link_response.text

                            try:
                                # 尝试 Base64 解码
                                decoded_content = base64.b64decode(link_content).decode('utf-8')
                                outfile.write(decoded_content + "\n")
                                print(f"  - 成功获取并解码链接内容。")
                            except base64.binascii.Error:
                                # 如果不是 Base64，则直接写入
                                outfile.write(link_content + "\n")
                                print(f"  - 成功获取链接内容 (非 Base64)。")
                            except UnicodeDecodeError:
                                # Base64解码后可能不是UTF-8，尝试其他编码或直接写入原始bytes
                                try:
                                    decoded_content = base64.b64decode(link_content).decode('latin-1') # 尝试 latin-1
                                    outfile.write(decoded_content + "\n")
                                    print(f"  - 成功获取并解码链接内容 (latin-1)。")
                                except Exception as e:
                                    outfile.write(f"无法解码内容 (Base64 或其他常见编码): {e}\n")
                                    print(f"  - 获取链接内容但无法解码: {e}")

                        except requests.exceptions.RequestException as e:
                            outfile.write(f"获取链接内容时发生错误: {e}\n")
                            print(f"  - 获取链接内容时发生错误: {e}")

                        # 因为只需要第一个，处理完后就可以跳出循环
                        break

            if link_found_and_processed:
                print(f"已找到并处理第一个符合条件的链接，内容已写入到文件: {output_filename}")
            else:
                print(f"在 URL: {markdown_url} 的 Markdown 文件中没有找到符合条件的链接。")

        else:
            print(f"在 URL: {markdown_url} 的 Markdown 文件中没有找到符合条件的链接。")

    except requests.exceptions.RequestException as e:
        print(f"获取 Markdown 文件内容时发生错误: {e}")

# 示例用法 (请替换成你的 Markdown 文件 URL)
markdown_url = "https://ghfast.top/https://raw.githubusercontent.com/mksshare/mksshare.github.io/main/README.md"
process_and_write_first_link_content(markdown_url)