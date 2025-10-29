#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新闻数据模型
"""

from datetime import datetime
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from models.analysis import AnalysisResult

class News:
    """新闻数据模型"""
    
    def __init__(self):
        self.id: Optional[int] = None
        self.url: str = ""
        self.title: str = ""
        self.summary: Optional[str] = None
        self.content: Optional[str] = None
        self.source_name: Optional[str] = None
        self.published_at: Optional[datetime] = None
        self.image_url: Optional[str] = None
        self.created_at: Optional[datetime] = None
        self.updated_at: Optional[datetime] = None
        self.analysis_result: Optional['AnalysisResult'] = None  # 关联的分析结果
    
    def to_dict(self) -> dict:
        """将新闻对象转换为字典"""
        return {
            'id': self.id,
            'url': self.url,
            'title': self.title,
            'summary': self.summary,
            'content': self.content,
            'source_name': self.source_name,
            'published_at': self.published_at.isoformat() if self.published_at else None,
            'image_url': self.image_url,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'News':
        """从字典创建新闻对象"""
        news = cls()
        news.id = data.get('id')
        news.url = data.get('url', '')
        news.title = data.get('title', '')
        news.summary = data.get('summary')
        news.content = data.get('content')
        news.source_name = data.get('source_name')
        news.image_url = data.get('image_url')
        
        # 处理日期时间字段
        for field in ['published_at', 'created_at', 'updated_at']:
            if data.get(field):
                try:
                    setattr(news, field, datetime.fromisoformat(data[field]))
                except (ValueError, TypeError):
                    setattr(news, field, None)
        
        return news
    
    def __repr__(self) -> str:
        return f"<News id={self.id} title='{self.title}' source='{self.source_name}'>"
    
    def is_black_swan(self) -> bool:
        """检查是否为黑天鹅事件"""
        return self.analysis_result is not None and self.analysis_result.is_black_swan
    
    def get_confidence(self) -> float:
        """获取分析置信度"""
        if self.analysis_result:
            return self.analysis_result.confidence
        return 0.0