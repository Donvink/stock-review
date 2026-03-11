"""报告生成模块"""
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd

from skills.stock_review.utils.logger import get_logger
from skills.stock_review.config.settings import Settings

class ReportGenerator:
    """报告生成器"""
    
    def __init__(self, config: Settings):
        self.config = config
        self.logger = get_logger(__name__)
    
    def create_market_summary(self, market_data: Dict, date: str) -> str:
        """
        创建市场数据汇总Markdown
        
        Args:
            market_data: 市场数据字典
            date: 日期
            
        Returns:
            Markdown格式的市场汇总
        """
        save_dir = self.config.data_dir / date
        file_path = save_dir / f"market_summary_{date}.md"
        
        if file_path.exists() and not market_data.get('cached', False):
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        
        index_df = market_data.get('index', pd.DataFrame())
        zt_df = market_data.get('zt', pd.DataFrame())
        dt_df = market_data.get('dt', pd.DataFrame())
        zb_df = market_data.get('zb', pd.DataFrame())
        up_count = market_data.get('up_count', 0)
        down_count = market_data.get('down_count', 0)
        top_amount = market_data.get('top_amount', pd.DataFrame())
        concept = market_data.get('concept', pd.DataFrame())
        concept_cons = market_data.get('concept_cons', [])
        lhb_df = market_data.get('lhb', pd.DataFrame())
        w1_df = market_data.get('watchlist1', pd.DataFrame())
        w2_df = market_data.get('watchlist2', pd.DataFrame())
        
        # 构建内容
        content = []
        content.append(f"# A股全市场复盘 {date}\n")
        
        # 市场快照
        if not index_df.empty:
            content.append("## 📊 市场核心快照")
            content.append(f"- **上证指数**: {index_df.iloc[0]['最新价']:.2f} ({index_df.iloc[0]['涨跌幅']:.2f}%)")
            if len(index_df) > 2:
                content.append(f"- **全市场成交总额**: {index_df.iloc[2]['成交额(亿元)']}")
            content.append(f"- **涨跌比**: {up_count} / {down_count}")
            content.append(f"- **涨停/跌停/炸板数**: {len(zt_df)} / {len(dt_df)} / {len(zb_df)}\n")
        
        # 成交额前二十
        if not top_amount.empty:
            content.append("## 🔍 成交额前二十个股")
            content.append(self._df_to_markdown(top_amount))
        
        # 概念板块
        if not concept.empty:
            content.append("## 🏆 概念板块分析")
            content.append("**前五概念板块**（按涨幅排序）")
            content.append(self._df_to_markdown(concept))
        
        # 板块成分股
        if concept_cons:
            content.append("### 各板块涨幅靠前个股")
            for i, cons_df in enumerate(concept_cons[:5]):
                if not cons_df.empty:
                    board_name = cons_df['所属板块'].iloc[0] if '所属板块' in cons_df.columns else f"板块{i+1}"
                    content.append(f"**{board_name}**")
                    content.append(self._df_to_markdown(cons_df))
        
        # 涨停炸板
        if not zt_df.empty:
            content.append("## 💥 涨停个股")
            content.append(self._df_to_markdown(zt_df))
        
        if not zb_df.empty:
            content.append("## 💔 炸板个股")
            content.append(self._df_to_markdown(zb_df))
        
        # 龙虎榜
        if not lhb_df.empty:
            content.append("## 🚀 龙虎榜")
            content.append(self._df_to_markdown(lhb_df))
        
        # Watchlist
        if not w1_df.empty:
            content.append("## ⭐ 重点个股 Watchlist")
            content.append("### 大额异动池")
            content.append(self._df_to_markdown(w1_df))
        
        if not w2_df.empty:
            content.append("### 风口涨停池")
            content.append(self._df_to_markdown(w2_df))
        
        content.append("\n---\n*数据来源：AKShare*")
        
        full_content = "\n\n".join(content)
        
        # 保存
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        self.logger.info(f"Market summary saved to {file_path}")
        return full_content
    
    def generate_all(self, market_data: Dict, market_summary: str, ai_analysis: Optional[str], date: str) -> Dict[str, Path]:
        """
        生成所有报告
        
        Returns:
            报告路径字典
        """
        save_dir = self.config.data_dir / date
        reports = {}
        
        # 市场汇总已生成
        reports['market_summary'] = save_dir / f"market_summary_{date}.md"
        
        # AI分析
        if ai_analysis:
            ai_path = save_dir / f"ai_analysis_{date}.md"
            with open(ai_path, 'w', encoding='utf-8') as f:
                f.write(ai_analysis)
            reports['ai_analysis'] = ai_path
        
        return reports
    
    def _df_to_markdown(self, df: pd.DataFrame) -> str:
        """DataFrame转Markdown表格"""
        if df.empty:
            return "暂无数据"
        
        # 限制列宽
        display_df = df.copy()
        for col in display_df.columns:
            if display_df[col].dtype == 'object':
                display_df[col] = display_df[col].astype(str).str[:30]
        
        return display_df.to_markdown(index=False, tablefmt="pipe")