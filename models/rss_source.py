#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS源数据模型
"""

from datetime import datetime
from typing import Optional

class RSSSource:
    """RSS源数据模型"""
    
    def __init__(self):
        self.id: Optional[int] = None
        self.name: str = ""
        self.url: str = ""
        self.enabled: bool = True
        self.update_interval: int = 3600  # 默认1小时
        self.last_fetched: Optional[datetime] = None
        self.created_at: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """将RSS源对象转换为字典"""
        return {
            'id': self.id,
            'name': self.name,
            'url': self.url,
            'enabled': self.enabled,
            'update_interval': self.update_interval,
            'last_fetched': self.last_fetched.isoformat() if self.last_fetched else None,
            'created_at': self.created_at.isoformat() if self.created_at else None
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'RSSSource':
        """从字典创建RSS源对象"""
        source = cls()
        source.id = data.get('id')
        source.name = data.get('name', '')
        source.url = data.get('url', '')
        source.enabled = data.get('enabled', True)
        source.update_interval = data.get('update_interval', 3600)
        
        # 处理日期时间字段
        for field in ['last_fetched', 'created_at']:
            if data.get(field):
                try:
                    setattr(source, field, datetime.fromisoformat(data[field]))
                except (ValueError, TypeError):
                    setattr(source, field, None)
        
        return source
    
    def is_due_for_update(self) -> bool:
        """检查是否到了更新时间"""
        if not self.enabled or not self.last_fetched:
            return True
        
        time_since_last_fetch = datetime.utcnow() - self.last_fetched
        return time_since_last_fetch.total_seconds() >= self.update_interval
    
    def __repr__(self) -> str:
        return f"<RSSSource id={self.id} name='{self.name}' url='{self.url}' enabled={self.enabled}>"