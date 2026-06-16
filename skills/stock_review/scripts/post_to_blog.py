from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple
import json

from utils.logger import get_logger
from config import Settings

# Static Chinese → English mapping for market-summary structural text.
# Only covers section headers, bullet labels, and table column names —
# numeric data and stock names are left untouched.
# Keys are sorted longest-first at runtime to avoid partial-match collisions.
_ZH_EN_MAP: dict[str, str] = {
    # ── Section headers ───────────────────────────────────────────────
    "## 📊 市场核心快照":         "## 📊 Market Snapshot",
    "- **上证指数**":             "- **Shanghai Composite (SSE)**",
    "- **深证指数**":             "- **Shenzhen Component (SZSE)**",
    "- **全市场成交总额**":        "- **Total Market Turnover**",
    "- **涨跌比**":               "- **Advance / Decline**",
    "- **涨停/跌停/炸板数**":      "- **Limit-Up / Limit-Down / Failed Board**",
    "## 🔍 成交额前二十个股":      "## 🔍 Top 20 Stocks by Turnover",
    "## 🏆 概念板块分析":          "## 🏆 Concept Sector Analysis",
    "**前五概念板块**（按涨幅排序）": "**Top 5 Concept Sectors** (by % gain)",
    "### 各板块涨幅靠前个股":       "### Top Gainers Within Each Sector",
    "## 💥 涨停个股":             "## 💥 Limit-Up Stocks",
    "## 💔 炸板个股":             "## 💔 Failed Limit-Up Stocks",
    "## 🚀 龙虎榜":               "## 🚀 Dragon-Tiger Board",
    "## ⭐ 重点个股 Watchlist":    "## ⭐ Key Stock Watchlist",
    "### 大额异动池":              "### High-Volume Momentum Pool",
    "### 风口涨停池":              "### Hot Limit-Up Pool",
    "暂无数据":                    "No data available",
    # ── Table column headers ──────────────────────────────────────────
    "成交额(亿元)":  "Turnover(100M¥)",  # longer first — must precede "成交额"
    "板块名称":      "Sector Name",
    "流通市值":      "Float Cap",
    "涨停统计":      "Limit-Up Record",
    "上涨家数":      "Advancing",
    "下跌家数":      "Declining",
    "换手率":        "Turnover Rate",
    "总市值":        "Mkt Cap",
    "连板数":        "Boards",
    "涨跌幅":        "Change%",
    "成交额":        "Turnover",
    "最新价":        "Price",
    "所属板块":      "Sector",
    "名称":          "Name",
    "代码":          "Code",
    "序号":          "No.",
}

class BlogPoster:
    """Hugo Blog Poster"""
    
    def __init__(self, config: Settings):
        self.config = config
        self.logger = get_logger(__name__)

    @staticmethod
    def localize_summary(market_summary: str) -> str:
        """Replace Chinese structural strings with English equivalents.

        Only section headers, bullet labels, and table column names are
        translated. Numeric data and stock names are left as-is.
        Longer keys are replaced before shorter ones to avoid partial matches.
        """
        result = market_summary
        for zh, en in sorted(_ZH_EN_MAP.items(), key=lambda kv: -len(kv[0])):
            result = result.replace(zh, en)
        return result

    def build_content(self, market_summary: str, ai_title: Optional[str], ai_tags: Optional[List[str]], ai_content: Optional[str]) -> Tuple[str, str]:
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
        ai_title = (ai_title or "A股市场复盘").strip(' \'“”"')
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
        # use local timezone; isoformat(timespec='seconds') always produces RFC3339-compliant +HH:MM
        now = datetime.now().astimezone()
        safe_now = now - timedelta(minutes=10)

        date_filename = safe_now.strftime("%Y-%m-%d")
        formatted_date = safe_now.isoformat(timespec='seconds')

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
    
