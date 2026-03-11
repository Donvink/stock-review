"""AI分析模块"""
from google import genai
from typing import Optional

from utils.logger import get_logger
from config import Settings

class MarketAnalyzer:
    """市场AI分析器"""
    
    def __init__(self, config: Settings):
        self.config = config
        self.logger = get_logger(__name__)
        self.client = None
        
        if config.gemini_api_key:
            self.client = genai.Client(api_key=config.gemini_api_key)
    
    def analyze(self, market_summary: str) -> Optional[str]:
        """
        使用AI分析市场数据
        
        Args:
            market_summary: 市场数据汇总Markdown
            
        Returns:
            AI分析结果Markdown
        """
        if not self.client:
            self.logger.warning("Gemini API key not set, skipping AI analysis")
            return None
        
        prompt = self._build_prompt(market_summary)
        
        try:
            response = self.client.models.generate_content(
                model='gemini-2.0-flash-exp',
                contents=prompt
            )
            
            self.logger.info("AI analysis completed successfully")
            return response.text
            
        except Exception as e:
            self.logger.error(f"AI analysis failed: {e}")
            return None
    
    def _build_prompt(self, market_summary: str) -> str:
        """构建AI提示词"""
        return f"""
        角色设定：你是一位拥有 20 年经验的 A 股资深策略分析师，擅长从成交量能、板块轮动和连板梯队中洞察市场情绪。

        任务描述：请基于下方提供的【当日复盘数据】，进行多维度复盘：

        1. 🚩 市场情绪诊断
        2. 💰 核心主线与资金流向
        3. 🪜 连板梯度与空间博弈
        4. ⚡ 重点异动个股分析
        5. 🧭 次日交易策略建议

        ---
        **📊 当日复盘数据内容如下**:
        {market_summary}

        要求：专业、客观、语言简练，避免模棱两可。输出格式使用 Markdown 标题和列表，增强可读性。
        """