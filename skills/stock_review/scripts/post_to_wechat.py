import requests
import json
from typing import Optional, Dict
from pathlib import Path
import markdown

from utils.logger import get_logger
from config import Settings

class WeChatPoster:
    """WeChat Official Account Poster"""
    
    def __init__(self, config: Settings):
        self.config = config
        self.logger = get_logger(__name__)
        self.access_token = None
        self.token_expires = 0
    
    def create_draft(self, market_summary: str, ai_analysis: Optional[str], date: str) -> Optional[str]:
        """
        Create a WeChat Official Account draft
        
        Args:
            market_summary: market summary Markdown
            ai_analysis: AI analysis Markdown
            date: date
            
        Returns:
            draft ID
        """
        if not self.config.has_wechat:
            self.logger.warning("WeChat credentials not configured")
            return None
        
        try:
            # get access token
            token = self._get_access_token()
            if not token:
                return None
            
            # create content
            content = self._build_content(market_summary, ai_analysis, date)
            
            # create draft
            url = f"https://api.weixin.qq.com/cgi-bin/draft/add?access_token={token}"
            
            data = {
                "articles": [{
                    "title": f"A股全市场复盘：{date} 深度解析及AI洞察",
                    "author": "Stock Review AI",
                    "content": content,
                    "thumb_media_id": self._get_thumb_media_id(),
                    "need_open_comment": 1,
                    "only_fans_can_comment": 0
                }]
            }
            
            response = requests.post(url, json=data)
            result = response.json()
            
            if result.get('errcode') == 0:
                media_id = result.get('media_id')
                self.logger.info(f"WeChat draft created: {media_id}")
                return media_id
            else:
                self.logger.error(f"Failed to create draft: {result}")
                return None
                
        except Exception as e:
            self.logger.error(f"WeChat draft creation failed: {e}")
            return None
    
    def _get_access_token(self) -> Optional[str]:
        """Get access token"""
        url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={self.config.wechat_app_id}&secret={self.config.wechat_app_secret}"
        
        try:
            response = requests.get(url)
            result = response.json()
            
            if 'access_token' in result:
                return result['access_token']
            else:
                self.logger.error(f"Failed to get access token: {result}")
                return None
        except Exception as e:
            self.logger.error(f"Access token request failed: {e}")
            return None
    
    def _build_content(self, market_summary: str, ai_analysis: Optional[str], date: str) -> str:
        """Build WeChat article HTML content"""
        # Convert Markdown to HTML
        md = markdown.Markdown(extensions=['extra', 'toc'])
        
        content_html = md.convert(market_summary)
        
        if ai_analysis:
            ai_html = md.convert(ai_analysis)
            content_html += f"<h2>🤖 AI深度分析与洞察</h2>{ai_html}"
        
        # add some basic styling for better display in WeChat articles
        style = """
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333; max-width: 100%; margin: 0 auto; padding: 15px; }
            h1 { font-size: 24px; border-left: 5px solid #1890ff; padding-left: 15px; margin: 20px 0; }
            h2 { font-size: 20px; border-bottom: 2px solid #f0f0f0; padding-bottom: 8px; margin: 20px 0 15px; }
            h3 { font-size: 18px; margin: 15px 0; }
            table { border-collapse: collapse; width: 100%; margin: 15px 0; font-size: 14px; }
            th { background: #f5f5f5; font-weight: 600; padding: 8px; text-align: left; border: 1px solid #e8e8e8; }
            td { padding: 8px; border: 1px solid #e8e8e8; }
            .stock-up { color: #f5222d; }
            .stock-down { color: #52c41a; }
        </style>
        """
        
        full_content = f"<!DOCTYPE html><html><head><meta charset='utf-8'>{style}</head><body>{content_html}<p style='color:#999;font-size:12px;text-align:center;margin-top:30px;'>数据来源：AKShare | 投资有风险，入市需谨慎</p></body></html>"
        
        return full_content
    
    def _get_thumb_media_id(self) -> str:
        """Get cover image media_id"""
        # This can return a fixed cover image ID
        return ""