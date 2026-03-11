import os
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from dotenv import load_dotenv

@dataclass
class Settings:
    """Configuration settings for Stock Review Skill"""
    
    # keys
    gemini_api_key: Optional[str] = None
    wechat_app_id: Optional[str] = None
    wechat_app_secret: Optional[str] = None
    
    # path configurations
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent)
    data_dir: Path = None
    content_dir: Path = None
    
    # execution settings
    max_retries: int = 3
    request_delay: float = 0.5
    backtrack_days: int = 20
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        config = config or {}
        
        # step 1. load .env files
        # priority: project root > skill root > user config > XDG config
        self._load_env_files()
        
        # step 2. load environment variables with fallback to config dict
        env_config = {
            'gemini_api_key': os.getenv('GEMINI_API_KEY'),
            'wechat_app_id': os.getenv('WECHAT_APP_ID'),
            'wechat_app_secret': os.getenv('WECHAT_APP_SECRET'),
            'max_retries': int(os.getenv('MAX_RETRIES', '3')),
            'request_delay': float(os.getenv('REQUEST_DELAY', '0.5')),
            'backtrack_days': int(os.getenv('BACKTRACK_DAYS', '20')),
        }
        
        # step 3. merge config with environment variables
        # priority: config > env vars > defaults
        for key, value in env_config.items():
            if value is not None and key not in config:
                config[key] = value
        
        # step 4. set attributes
        self.gemini_api_key = config.get('gemini_api_key')
        self.wechat_app_id = config.get('wechat_app_id')
        self.wechat_app_secret = config.get('wechat_app_secret')
        self.max_retries = int(config.get('max_retries', self.max_retries))
        self.request_delay = float(config.get('request_delay', self.request_delay))
        self.backtrack_days = int(config.get('backtrack_days', self.backtrack_days))
        
        # step 5. set paths
        self.base_dir = Path(config.get('base_dir', self.base_dir))
        self.data_dir = self.base_dir / 'data'
        self.content_dir = self.base_dir / 'content' / 'posts'
        
        # make sure directories exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.content_dir.mkdir(parents=True, exist_ok=True)
        
        # step 6. validate necessary configurations
        self._validate()
    
    def _load_env_files(self):
        """
        Load .env files in order of priority:
        
        # search order:
        # 1. project root (.env)
        # 2. skill root (.env)
        # 3. user config directory (~/.openclaw/skills/stock_review/.env)
        # 4. XDG config directory (~/.config/stock_review/.env)
        """
        
        search_paths = [
            Path.cwd() / '.env',                                            # project root
            Path(__file__).parent.parent / '.env',                          # skill root
            Path.home() / '.openclaw' / 'skills' / 'stock_review' / '.env', # user config directory
            Path(os.getenv('XDG_CONFIG_HOME', Path.home() / '.config')) / 
                'stock_review' / '.env',                                    # XDG config directory
        ]
        
        loaded_files = []
        for env_path in search_paths:
            if env_path.exists():
                load_dotenv(env_path, override=False)
                loaded_files.append(str(env_path))
        
        if loaded_files:
            print(f"📁 load .env files: {', '.join(loaded_files)}")
        else:
            print("ℹ️ .env not found, using system environment variables")
    
    def _validate(self):
        """Validate necessary configurations"""
        if not self.gemini_api_key:
            print(" ⚠️ Warning: GEMINI_API_KEY not set, AI analysis will be unavailable")
        
        if not self.has_wechat:
            print(" ⚠️ Warning: WeChat configuration not set, WeChat functionality will be unavailable")
    
    @property
    def has_wechat(self) -> bool:
        """WeChat is available if both app_id and app_secret are set"""
        return bool(self.wechat_app_id and self.wechat_app_secret)