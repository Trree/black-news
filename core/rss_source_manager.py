#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS源管理器模块
负责RSS源的加载、管理和提供接口
"""

import json
import logging
import os
from typing import List, Optional, Dict, Any
from datetime import datetime

from models.rss_source import RSSSource

logger = logging.getLogger(__name__)


class RSSSourceManager:
    """RSS源管理器类"""
    
    def __init__(self, sources_file: str = "rss_sources.json"):
        self.sources_file = sources_file
    
    def _load_rss_sources_from_file(self) -> List[Dict[str, Any]]:
        """从JSON文件加载RSS源配置"""
        try:
            if not os.path.exists(self.sources_file):
                logger.warning(f"RSS源配置文件不存在: {self.sources_file}")
                return []
            
            with open(self.sources_file, 'r', encoding='utf-8') as f:
                sources_data = json.load(f)
            
            return sources_data
            
        except Exception as e:
            logger.error(f"加载RSS源配置文件失败: {e}")
            return []
    
    def get_rss_sources(self) -> List[RSSSource]:
        """获取所有RSS源"""
        try:
            sources_data = self._load_rss_sources_from_file()
            logger.info(f"从文件加载的RSS源数据: {sources_data}")
            sources = []
            for source_data in sources_data:
                # 转换JSON数据为RSSSource对象
                source = RSSSource()
                source.id = source_data.get('id')
                source.name = source_data.get('name', '')
                source.url = source_data.get('url', '')
                source.enabled = source_data.get('enabled', True)
                source.update_interval = source_data.get('update_interval', 3600)
                
                # 处理日期时间字段
                if source_data.get('last_fetched'):
                    try:
                        source.last_fetched = datetime.fromisoformat(source_data['last_fetched'])
                    except (ValueError, TypeError):
                        source.last_fetched = None
                
                if source_data.get('created_at'):
                    try:
                        source.created_at = datetime.fromisoformat(source_data['created_at'])
                    except (ValueError, TypeError):
                        source.created_at = None
                
                sources.append(source)
            
            return sources
            
        except Exception as e:
            logger.error(f"获取RSS源列表失败: {e}")
            return []
    
    def get_enabled_rss_sources(self) -> List[RSSSource]:
        """获取启用的RSS源"""
        try:
            sources = self.get_rss_sources()
            enabled_sources = [source for source in sources if source.enabled]
            logger.info(f"启用的RSS源数量: {len(enabled_sources)}")
            return enabled_sources
            
        except Exception as e:
            logger.error(f"获取启用的RSS源列表失败: {e}")
            return []
    
    def get_rss_source_by_id(self, source_id: int) -> Optional[RSSSource]:
        """根据ID获取RSS源"""
        try:
            sources = self.get_rss_sources()
            for source in sources:
                if source.id == source_id:
                    return source
            return None
            
        except Exception as e:
            logger.error(f"获取RSS源失败: {e}")
            return None


# 创建全局实例
_rss_source_manager = RSSSourceManager()


def get_rss_source_manager() -> RSSSourceManager:
    """获取RSS源管理器实例"""
    return _rss_source_manager


def get_enabled_rss_sources() -> List[RSSSource]:
    """获取启用的RSS源（便捷函数）"""
    return get_rss_source_manager().get_enabled_rss_sources()


def get_all_rss_sources() -> List[RSSSource]:
    """获取所有RSS源（便捷函数）"""
    return get_rss_source_manager().get_rss_sources()