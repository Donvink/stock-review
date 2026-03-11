"""tool for logging in stock review skill"""
from ctypes import Union
import logging
import sys
from pathlib import Path
from datetime import datetime
from pyparsing import Optional

def setup_logger(
        name: str,
        level=logging.INFO,
        log_to_file: bool = False,
        log_dir: Optional[Union[str, Path]] = None
    ) -> logging.Logger:
    """Setting up a logger with console and file handlers"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(level)
        
        # console handler, log format: time - name - level - message
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(level)
        console.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(console)
        
        # file handler, log directory is logs, file name is stock_review_YYYYMMDD.log
        if log_to_file:
            if log_dir is None:
                log_dir = Path(__file__).parent.parent.parent.parent / 'logs'
            else:
                log_dir = Path(log_dir)
            log_dir.mkdir(exist_ok=True)
            
            log_file = log_dir / f"stock_review_{datetime.now().strftime('%Y%m%d')}.log"
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(level)
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            logger.addHandler(file_handler)
            logger.info(f"Log file saved: {log_file}")
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """Get a logger by name, if not exist, create one with setup_logger"""
    return logging.getLogger(name)