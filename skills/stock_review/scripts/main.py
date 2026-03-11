import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import json

# add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fetch_data import DataFetcher
from analyze import MarketAnalyzer
from generate_report import ReportGenerator
from post_to_blog import BlogPoster
from post_to_wechat import WeChatPoster
from utils.logger import setup_logger
from config import Settings

class StockReview:
    """A-share market review and analysis skill main class"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the stock review
        
        Args:
            config: configurations, if None, will load from environment variables and .env files
        """
        self.config = Settings(config or {})
        self.logger = setup_logger(__name__)
        
        # initialize components
        self.data_fetcher = DataFetcher(self.config)
        self.analyzer = MarketAnalyzer(self.config)
        self.report_generator = ReportGenerator(self.config)
        self.blog_poster = BlogPoster(self.config)
        self.wechat_poster = WeChatPoster(self.config) if self.config.has_wechat else None
        
        self.logger.info("StockReview initialized successfully")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the full review and analysis process
        
        Args:
            context: execution context, can include:
                - date: specified date (YYYYMMDD)
                - force_refresh: force refresh data
                - skip_ai: skip AI analysis
                - platforms: list of platforms to publish to ['hugo', 'wechat']
        
        Returns:
            execution result dictionary
        """
        self.logger.info(f"Starting stock review execution with context: {context}")
        
        try:
            # 1. prepare parameters
            date = context.get('date')
            force_refresh = context.get('force_refresh', False)
            skip_ai = context.get('skip_ai', False)
            platforms = context.get('platforms', ['hugo'])
            
            # 2. get latest date if not specified
            if not date:
                date = self.data_fetcher.get_latest_date()
                self.logger.info(f"Using latest available date: {date}")
            
            # 3. get market data
            market_data = self.data_fetcher.fetch_all(date, force_refresh)
            if not market_data:
                error_msg = "Failed to fetch market data"
                self.logger.error(error_msg)
                raise Exception(error_msg)
            
            # 4. generate market summary
            market_summary = self.report_generator.create_market_summary(
                market_data, date
            )
            
            # 5. AI analysis (optional)
            ai_analysis = None
            if not skip_ai and self.config.gemini_api_key:
                ai_analysis = self.analyzer.analyze(market_summary)
                self.logger.info("AI analysis completed")
            
            # 6. generate reports
            reports = self.report_generator.generate_all(
                market_data, market_summary, ai_analysis, date
            )
            
            # 7. post to platforms
            results = {
                "date": date,
                "market_summary": market_summary[:500] + "...",  # 摘要
                "reports": list(reports.keys()),
                "published": []
            }
            
            if 'hugo' in platforms:
                post_path = self.blog_poster.create_post(
                    market_summary, ai_analysis, date
                )
                results["published"].append({
                    "platform": "hugo",
                    "path": post_path
                })
            
            if 'wechat' in platforms and self.wechat_poster:
                draft_id = self.wechat_poster.create_draft(
                    market_summary, ai_analysis, date
                )
                results["published"].append({
                    "platform": "wechat",
                    "draft_id": draft_id
                })
            
            self.logger.info(f"Stock review completed successfully for {date},\
                                summary: {market_summary[:100]}..., \
                                    published to: {[p['platform'] for p in results['published']]}")
            return results
            
        except Exception as e:
            self.logger.error(f"Execution failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def validate(self) -> bool:
        """
        Validate if the skill configuration is valid by
        checking necessary parameters and testing data fetching
        
        Returns:
            Configuration is valid
        """
        try:
            # Check necessary configuration
            if not self.config.gemini_api_key:
                self.logger.warning("GEMINI_API_KEY not set")
            
            # # Try to connect to data source
            # test_date = datetime.now().strftime("%Y%m%d")
            # self.data_fetcher.fetch_all(test_date, force_refresh=True)
            
            return True
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            return False