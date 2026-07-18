import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class Settings:
    """Configuration settings for Stock Review Skill"""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        config = config or {}
        
        # step 1. load .env files
        self._load_env_files()
        
        # step 2. load environment variables with fallback to config dict
        env_config = {
            'gemini_api_key': os.getenv('GEMINI_API_KEY'),
            'wechat_app_id': os.getenv('WECHAT_APP_ID'),
            'wechat_app_secret': os.getenv('WECHAT_APP_SECRET'),
        }
        
        # step 3. merge config with environment variables
        # priority: config > env vars > defaults
        for key, value in env_config.items():
            if value is not None and key not in config:
                config[key] = value
        
        # step 4. set attributes
        # Extract each top-level configuration block
        review_config = config.get('review', {})
        paths_config = config.get('paths', {})
        params_config = config.get('parameters', {})
        models_config = config.get('models', {})

        # set values from review block
        self.date = review_config.get('date', None)
        self.force_refresh = review_config.get('force_refresh', False)
        self.skip_ai_analysis = review_config.get('skip_ai_analysis', False)
        self.platforms = review_config.get('platforms', ['hugo'])

        # set values from paths block
        self.base_dir = Path(__file__).parent.parent.parent.parent
        data_dir_value = paths_config.get('data_dir')
        self.data_dir = self.base_dir / 'data' if data_dir_value is None else Path(data_dir_value)
        self.content_dir = self.base_dir / 'content'
        # make sure directories exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.content_dir.mkdir(parents=True, exist_ok=True)

        # set values from parameters block
        self.max_retries = int(params_config.get('max_retries', 3))
        self.request_delay = float(params_config.get('request_delay', 0.5))
        self.backtrack_days = int(params_config.get('backtrack_days', 0))
        # backoff base delay (seconds) for retrying a failed Gemini call
        # (503 "model overloaded" tends to clear after tens of seconds; kept
        # separate from request_delay, which throttles AKShare data fetches)
        self.ai_retry_delay = float(params_config.get('ai_retry_delay', 20.0))

        # set values from models block
        self.model_name = models_config.get('model_name', 'gemini-flash-latest')

        # step 5. set api keys from environment variables or config
        self.gemini_api_key = config.get('gemini_api_key')
        self.wechat_app_id = config.get('wechat_app_id')
        self.wechat_app_secret = config.get('wechat_app_secret')
        
        # step 6. validate necessary configurations
        self._validate()
    
    def _load_env_files(self):
        """
        Load .env files from lowest to highest priority so that each
        subsequent file can override previous values (override=True).

        Priority (highest → lowest):
        1. project root (.env)          — loaded last, wins
        2. skill root (.env)
        3. user config directory (~/.openclaw/skills/stock_review/.env)
        4. XDG config directory (~/.config/stock_review/.env)  — loaded first
        """
        search_paths = [
            Path(os.getenv('XDG_CONFIG_HOME', Path.home() / '.config')) /
                'stock_review' / '.env',                                     # lowest priority
            Path.home() / '.openclaw' / 'skills' / 'stock_review' / '.env',
            Path(__file__).parent.parent / '.env',                           # skill root
            Path.cwd() / '.env',                                             # highest priority
        ]

        loaded_files = []
        for env_path in search_paths:
            if env_path.exists():
                load_dotenv(env_path, override=True)
                loaded_files.append(str(env_path))

        if loaded_files:
            logger.info(f"Loaded .env files: {', '.join(loaded_files)}")
        else:
            logger.info(".env not found, using system environment variables")
    
    def _validate(self):
        """Validate necessary configurations"""
        if not self.gemini_api_key:
            logger.warning("GEMINI_API_KEY not set, AI analysis will be unavailable")

        if not self.has_wechat:
            logger.warning("WeChat configuration not set, WeChat functionality will be unavailable")
    
    @property
    def has_wechat(self) -> bool:
        """WeChat is available if both app_id and app_secret are set"""
        return bool(self.wechat_app_id and self.wechat_app_secret)