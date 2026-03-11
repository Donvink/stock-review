"""测试数据获取模块"""
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.fetch_data import DataFetcher
from scripts.config import Settings

class TestDataFetcher:
    """测试数据获取器"""
    
    def setup_method(self):
        self.config = Settings({})
        self.fetcher = DataFetcher(self.config)
    
    def test_format_value(self):
        """测试数值格式化"""
        assert self.fetcher._format_value(100000000) == "1.00亿"
        assert self.fetcher._format_value(50000) == "5.00万"
        assert self.fetcher._format_value(123) == "123.00"
    
    def test_get_latest_date(self):
        """测试获取最新日期"""
        date = self.fetcher.get_latest_date()
        assert len(date) == 8
        assert date.isdigit()