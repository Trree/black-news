#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析结果数据模型
"""

from datetime import datetime
from typing import Optional

class AnalysisResult:
    """分析结果数据模型"""
    
    def __init__(self):
        self.id: Optional[int] = None
        self.news_id: Optional[int] = None
        self.is_black_swan: bool = False
        self.surprise_score: int = 0  # 1-10
        self.impact_score: int = 0    # 1-10
        self.analysis_reason: Optional[str] = None
        self.confidence: float = 0.0  # 0.0-1.0
        self.analyzed_at: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        """将分析结果对象转换为字典"""
        return {
            'id': self.id,
            'news_id': self.news_id,
            'is_black_swan': self.is_black_swan,
            'surprise_score': self.surprise_score,
            'impact_score': self.impact_score,
            'analysis_reason': self.analysis_reason,
            'confidence': self.confidence,
            'analyzed_at': self.analyzed_at.isoformat() if self.analyzed_at else None
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'AnalysisResult':
        """从字典创建分析结果对象"""
        result = cls()
        result.id = data.get('id')
        result.news_id = data.get('news_id')
        result.is_black_swan = data.get('is_black_swan', False)
        result.surprise_score = data.get('surprise_score', 0)
        result.impact_score = data.get('impact_score', 0)
        result.analysis_reason = data.get('analysis_reason')
        result.confidence = data.get('confidence', 0.0)
        
        # 处理日期时间字段
        if data.get('analyzed_at'):
            try:
                result.analyzed_at = datetime.fromisoformat(data['analyzed_at'])
            except (ValueError, TypeError):
                result.analyzed_at = None
        
        return result
    
    def calculate_black_swan_score(self) -> float:
        """计算黑天鹅综合评分"""
        # 基于意外程度、影响程度和置信度计算综合评分
        surprise_weight = 0.4
        impact_weight = 0.4
        confidence_weight = 0.2
        
        normalized_surprise = self.surprise_score / 10.0
        normalized_impact = self.impact_score / 10.0
        
        score = (
            surprise_weight * normalized_surprise +
            impact_weight * normalized_impact +
            confidence_weight * self.confidence
        )
        
        return round(score * 100, 2)  # 转换为0-100分
    
    def __repr__(self) -> str:
        return f"<AnalysisResult id={self.id} news_id={self.news_id} is_black_swan={self.is_black_swan}>"