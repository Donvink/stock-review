import markdown
import re
import requests
import json
import os


def convert_md_to_wechat_html(md_content):
    # --- 修复 1: 剔除 Markdown 元数据 (Frontmatter) ---
    # 匹配开头两个 --- 之间的所有内容并删除
    md_content = re.sub(r'^---.*?---', '', md_content, flags=re.DOTALL | re.MULTILINE)

    # 1. 定义更严谨的内联样式
    styles = {
        'h2': 'margin: 25px 0 15px; padding-left: 10px; border-left: 5px solid #07C160; font-size: 19px; font-weight: bold; color: #333; line-height: 1.5;',
        'p': 'margin: 12px 0; line-height: 1.7; color: #3f3f3f; font-size: 15px; text-align: justify;',
        'table': 'width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 12px; table-layout: fixed;',
        'th': 'background-color: #f1f1f1; border: 1px solid #dfe2e5; padding: 10px; font-weight: bold; color: #555;',
        'td': 'border: 1px solid #dfe2e5; padding: 10px; text-align: left; word-break: break-all;',
        'strong': 'color: #d63031; font-weight: bold;',
        'blockquote': 'margin: 15px 0; padding: 15px; border-left: 4px solid #07C160; background: #f8f8f8; color: #666;',
        'ul': 'margin: 10px 0; padding-left: 20px; list-style-type: disc;', # 修复列表显示
        'li': 'margin: 8px 0; line-height: 1.6; color: #3f3f3f; font-size: 15px;' # 修复列表间距
    }

    # 2. 转换 Markdown
    html = markdown.markdown(md_content, extensions=['tables', 'fenced_code'])

    # 3. 注入内联样式
    for tag, style in styles.items():
        # 优化替换逻辑：确保只替换标签本身
        html = html.replace(f'<{tag}>', f'<{tag} style="{style}">')
        html = re.sub(rf'<{tag}\s', f'<{tag} style="{style}" ', html)

    # --- 修复 2: 特殊处理无序列表的小圆点 ---
    # 微信对 li 标签的支持有时会丢掉圆点，我们可以手动给 li 标签内加一个样式化的符号
    html = html.replace('<li style="', '<li style="list-style-position: inside; ')

    # 4. 颜色与关键词优化
    html = html.replace('涨停', '<span style="color: #e84118; font-weight: bold;">涨停</span>')
    html = html.replace('跌停', '<span style="color: #4cd137; font-weight: bold;">跌停</span>')
    html = html.replace('炸板', '<span style="color: #fa8231; font-weight: bold;">炸板</span>')

    # 5. 外壳封装，强制列表符号显示
    final_html = f"""
    <div style="font-family: -apple-system-font, system-ui, sans-serif; letter-spacing: 0.5px; padding: 10px;">
        {html}
    </div>
    """
    return final_html

def get_access_token():
    appid = os.getenv("WECHAT_APPID")
    secret = os.getenv("WECHAT_SECRET")
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={appid}&secret={secret}"
    res = requests.get(url).json()
    token = res.get("access_token")
    if not token:
        print(f"❌ 获取 Token 失败: {res}")
    return token

def upload_image_as_thumb(access_token, image_path):
    """上传封面图并返回 media_id"""
    if not os.path.exists(image_path):
        print(f"❌ 找不到封面图片: {image_path}")
        return None
        
    # 微信上传永久素材接口（type 为 image）
    url = f"https://api.weixin.qq.com/cgi-bin/material/add_material?access_token={access_token}&type=image"
    
    with open(image_path, 'rb') as f:
        files = {'media': f}
        # 注意：这里是 multipart/form-data
        res = requests.post(url, files=files).json()
        
    media_id = res.get("media_id")
    if media_id:
        print(f"✅ 封面图上传成功: {media_id}")
    else:
        print(f"❌ 封面图上传失败: {res}")
    return media_id

def upload_to_wechat_draft(title, content_html, thumb_media_id):
    access_token = get_access_token()
    if not access_token or not thumb_media_id:
        return

    draft_url = f"https://api.weixin.qq.com/cgi-bin/draft/add?access_token={access_token}"
    
    data = {
        "articles": [
            {
                "title": title,
                "author": "AI复盘助手",
                "digest": "今日A股深度复盘与AI策略预测",
                "content": content_html,
                "thumb_media_id": thumb_media_id, # 使用上传得到的真实 ID
                "show_cover_pic": 1,
                "need_open_comment": 1
            }
        ]
    }

    response = requests.post(
        draft_url, 
        data=json.dumps(data, ensure_ascii=False).encode('utf-8')
    )
    
    result = response.json()
    if "media_id" in result:
        print(f"✅ 草稿上传成功！请登录后台查看。")
    else:
        print(f"❌ 上传失败: {result}")

if __name__ == "__main__":
    # 路径配置
    md_path = 'content/posts/stock-analysis-2026-02-17.md'
    img_path = 'content/images/demo.jpg' # 图片路径
    
    with open(md_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 转换 HTML
    wechat_ready_html = convert_md_to_wechat_html(content)
    
    # 执行上传
    token = get_access_token()
    if token:
        # 1. 先传图片拿 ID
        thumb_id = upload_image_as_thumb(token, img_path)
        # 2. 再传草稿
        if thumb_id:
            upload_to_wechat_draft("2026-02-17 A股复盘报告", wechat_ready_html, thumb_id)