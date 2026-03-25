from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
import json

from utils.logger import get_logger
from config import Settings

class BlogPoster:
    """Hugo Blog Poster"""
    
    def __init__(self, config: Settings):
        self.config = config
        self.logger = get_logger(__name__)

    def build_content(self, market_summary: str, ai_title: str, ai_tags: list, ai_content: str) -> Tuple[str, str]:
        """
        Create a Hugo blog post with market summary and AI analysis

        Args:
            market_summary: market summary Markdown
            ai_title: title for the AI analysis section
            ai_content: AI analysis Markdown
            lang: language code ("zh" or "en")

        Returns:
            display_title: title for the blog post
            markdown_content: full Markdown content for the post
        """
        ai_title = ai_title.strip().strip('"').strip("'").strip('“').strip('”')
        final_tags = ["每日复盘"] + (ai_tags if ai_tags else [])
        final_tags = list(dict.fromkeys(final_tags))[:6]
        ai_header = "## 🤖 AI 深度分析与洞察"
        footer = """
---
注：
1. 数据来源：AKShare。
2. 本文由AI辅助生成，旨在提供市场洞察和数据分析，非投资建议。
3. 声明：投资有风险，入市需谨慎。本文内容仅供参考，不构成任何投资建议或推荐。请根据自身情况做出独立判断。
"""
        # Concatenate market summary and AI content with proper formatting
        # use local timezone
        now = datetime.now().astimezone()
        safe_now = now - timedelta(minutes=10)

        date_filename = safe_now.strftime("%Y-%m-%d")
        formatted_date = safe_now.strftime("%Y-%m-%dT%H:%M:%S%z")

        # RFC3339 format requires a colon in the timezone offset, e.g. +08:00 instead of +0800
        if len(formatted_date) > 5 and formatted_date[-5:-4] not in '+-':
            formatted_date = formatted_date[:-2] + ':' + formatted_date[-2:]

        display_title = f"{date_filename}-{ai_title}"
        safe_title = json.dumps(display_title, ensure_ascii=False)

        full_content = f"""---
title: {safe_title}
date: {formatted_date}
tags: {json.dumps(final_tags, ensure_ascii=False)}
categories: ["每日更新"]
showToc: true
draft: false
---

{market_summary}

"""

        if ai_content:
            full_content += f"""
{ai_header}

{ai_content}
"""

        full_content += footer

        return date_filename, full_content

    def save_post(self, date_filename: str, content: str, lang: str = "zh") -> Path:
        """
        仅负责持久化：确定路径、组装 Front Matter 并写入文件
        """
        target_dir = self.config.content_dir / lang / "posts"
        target_dir.mkdir(parents=True, exist_ok=True)
        
        filename = target_dir / f"stock-analysis-{date_filename}.md"
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
            
        self.logger.info(f"Post saved to: {filename}")
        return filename
    
    def create_post(self, market_summary: str, ai_title: str, ai_content: str, date: str) -> Path:
        """
        Create a Hugo blog post with market summary and AI analysis
        
        Args:
            market_summary: market summary Markdown
            ai_analysis: AI analysis Markdown
            date: date
            
        Returns:
            post path
        """
        # use local timezone
        now = datetime.now().astimezone()
        safe_now = now - timedelta(minutes=10)

        date_filename = safe_now.strftime("%Y-%m-%d")
        formatted_date = safe_now.strftime("%Y-%m-%dT%H:%M:%S%z")

        # RFC3339 format requires a colon in the timezone offset, e.g. +08:00 instead of +0800
        if len(formatted_date) > 5 and formatted_date[-5:-4] not in '+-':
            formatted_date = formatted_date[:-2] + ':' + formatted_date[-2:]

        # print(f"time: {formatted_date}")
        
        filename = self.config.content_dir / f"stock-analysis-{date_filename}.md"
        
        content = f"""---
title: "{date_filename}-{ai_title}"
date: {formatted_date}
tags: ["每日复盘", "重点个股", "行业板块", "市场分析"]
categories: ["每日更新"]
showToc: true
draft: false
---

{market_summary}

"""

        if ai_content:
            content += f"""
## 🤖 AI 深度分析与洞察

{ai_content}
"""

        content += """
---
注：
1. 数据来源：AKShare。
2. 本文由AI辅助生成，旨在提供市场洞察和数据分析，非投资建议。
3. 声明：投资有风险，入市需谨慎。本文内容仅供参考，不构成任何投资建议或推荐。请根据自身情况做出独立判断。
"""
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        
        self.logger.info(f"Hugo post created: {filename}")
        return filename