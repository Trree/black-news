#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS服务模块
"""

import logging
from typing import Dict, Any

from core.database import get_db_manager
from core.rss_parser import RSSParser
from core.rss_source_manager import get_rss_source_manager

logger = logging.getLogger(__name__)

class RSSService:
    """RSS服务类"""
    
    def __init__(self, config_manager=None):
        self.db_manager = get_db_manager()
        self.config_manager = config_manager
        self.rss_parser = RSSParser(config_manager)
        self.rss_source_manager = get_rss_source_manager()


    def get_monitor_metrics(self) -> Dict[str, Any]:
        """获取监控指标"""
        return self.rss_parser.get_monitor_metrics()
    
    def get_health_status(self) -> str:
        """获取健康状态"""
        return self.rss_parser.get_health_status()
