#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析服务模块
"""

import asyncio
import json
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from models.news import News
from models.analysis import AnalysisResult
from core.database import get_db_manager
from core.llm_analyzer import LLMAnalyzer

logger = logging.getLogger(__name__)

class AnalysisService:
    """分析服务类"""
    
    def __init__(self, config_manager=None):
        self.db_manager = get_db_manager()
        self.config_manager = config_manager
        self.llm_analyzer = LLMAnalyzer(config_manager, self.db_manager)
        self.logger = logging.getLogger(__name__)
    
    def get_unanalyzed_news(self, limit: int = 10) -> List[News]:
        """获取未分析的新闻"""
        try:
            rows = self.db_manager.fetchall(
                """
                SELECT * FROM news 
                WHERE id NOT IN (SELECT news_id FROM analysis_results)
                ORDER BY published_at DESC
                LIMIT ?
                """,
                (limit,)
            )
            
            news_list = []
            for row in rows:
                news = News.from_dict(dict(row))
                news_list.append(news)
            
            return news_list
            
        except Exception as e:
            self.logger.error(f"获取未分析新闻失败: {e}")
            return []
    
    def get_analysis_result(self, news_id: int) -> Optional[AnalysisResult]:
        """获取新闻的分析结果"""
        try:
            row = self.db_manager.fetchone(
                "SELECT * FROM analysis_results WHERE news_id = ?",
                (news_id,)
            )
            
            if not row:
                return None
            
            return AnalysisResult.from_dict(dict(row))
            
        except Exception as e:
            self.logger.error(f"获取分析结果失败: {e}")
            return None
    
    async def analyze_news(self, news: News) -> Optional[AnalysisResult]:
        """分析单条新闻"""
        try:
            # 使用LLM分析器分析新闻
            analysis_result = await self.llm_analyzer.analyze_single_news(news)
            
            # 保存分析结果
            if analysis_result:
                analysis_result.news_id = news.id
                await self._save_analysis_result(analysis_result)
                return analysis_result
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"分析新闻失败 {news.id}: {e}")
            return None
    
    async def analyze_news_batch(self, news_list: List[News]) -> List[AnalysisResult]:
        """批量分析新闻"""
        try:
            # 获取批处理大小配置
            batch_size = 10
            if self.config_manager:
                analysis_config = self.config_manager.get('analysis_config', {})
                batch_size = analysis_config.get('batch_size', 10)
            
            # 批量分析
            results = await self.llm_analyzer.analyze_news_batch(news_list, batch_size)
            
            # 保存分析结果
            saved_results = []
            for i, result in enumerate(results):
                if isinstance(result, AnalysisResult):
                    result.news_id = news_list[i].id
                    await self._save_analysis_result(result)
                    saved_results.append(result)
                else:
                    self.logger.error(f"分析失败: {result}")
            
            return saved_results
            
        except Exception as e:
            self.logger.error(f"批量分析新闻失败: {e}")
            return []
    
    async def analyze_unanalyzed_news(self, batch_size: int = 10) -> Dict[str, Any]:
        """分析所有未分析的新闻"""
        try:
            # 获取未分析的新闻
            unanalyzed_news = self.get_unanalyzed_news(batch_size)
            
            if not unanalyzed_news:
                return {
                    'success': True,
                    'analyzed_count': 0,
                    'message': '没有未分析的新闻'
                }
            
            # 批量分析
            results = await self.analyze_news_batch(unanalyzed_news)
            
            return {
                'success': True,
                'analyzed_count': len(results),
                'total_requested': len(unanalyzed_news),
                'message': f'成功分析 {len(results)} 条新闻'
            }
            
        except Exception as e:
            self.logger.error(f"分析未分析新闻失败: {e}")
            return {
                'success': False,
                'analyzed_count': 0,
                'error': str(e)
            }
    
    async def _save_analysis_result(self, analysis_result: AnalysisResult) -> bool:
        """保存分析结果"""
        try:
            self.db_manager.execute(
                """
                INSERT OR REPLACE INTO analysis_results 
                (news_id, is_black_swan, surprise_score, impact_score, analysis_reason, confidence, analyzed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    analysis_result.news_id,
                    analysis_result.is_black_swan,
                    analysis_result.surprise_score,
                    analysis_result.impact_score,
                    analysis_result.analysis_reason,
                    analysis_result.confidence,
                    analysis_result.analyzed_at or datetime.utcnow()
                )
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"保存分析结果失败: {e}")
            return False
    
    def get_black_swan_statistics(self) -> Dict[str, Any]:
        """获取黑天鹅统计信息"""
        try:
            # 总分析数
            total_analyzed = self.db_manager.fetchone(
                "SELECT COUNT(*) as count FROM analysis_results"
            )
            
            # 黑天鹅事件数
            black_swan_count = self.db_manager.fetchone(
                "SELECT COUNT(*) as count FROM analysis_results WHERE is_black_swan = 1"
            )
            
            # 平均分数
            avg_scores = self.db_manager.fetchone(
                """
                SELECT AVG(surprise_score) as avg_surprise, AVG(impact_score) as avg_impact, AVG(confidence) as avg_confidence
                FROM analysis_results
                """
            )
            
            # 按来源统计
            source_stats = self.db_manager.fetchall(
                """
                SELECT n.source_name, COUNT(ar.id) as total, SUM(ar.is_black_swan) as black_swan_count
                FROM analysis_results ar
                JOIN news n ON ar.news_id = n.id
                GROUP BY n.source_name
                ORDER BY black_swan_count DESC
                """
            )
            
            total = total_analyzed['count'] if total_analyzed else 0
            black_swan = black_swan_count['count'] if black_swan_count else 0
            
            return {
                'total_analyzed': total,
                'black_swan_count': black_swan,
                'black_swan_ratio': round(black_swan / total * 100, 2) if total > 0 else 0,
                'average_scores': {
                    'surprise': round(avg_scores['avg_surprise'], 2) if avg_scores['avg_surprise'] else 0,
                    'impact': round(avg_scores['avg_impact'], 2) if avg_scores['avg_impact'] else 0,
                    'confidence': round(avg_scores['avg_confidence'], 2) if avg_scores['avg_confidence'] else 0
                },
                'source_statistics': [
                    {
                        'source': row['source_name'],
                        'total': row['total'],
                        'black_swan': row['black_swan_count'],
                        'ratio': round(row['black_swan_count'] / row['total'] * 100, 2) if row['total'] > 0 else 0
                    }
                    for row in source_stats
                ]
            }
            
        except Exception as e:
            self.logger.error(f"获取黑天鹅统计信息失败: {e}")
            return {
                'total_analyzed': 0,
                'black_swan_count': 0,
                'black_swan_ratio': 0,
                'average_scores': {
                    'surprise': 0,
                    'impact': 0,
                    'confidence': 0
                },
                'source_statistics': []
            }
    
    def get_analysis_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        """获取分析日志"""
        try:
            rows = self.db_manager.fetchall(
                """
                SELECT ar.*, n.title as news_title, n.source_name
                FROM analysis_results ar
                JOIN news n ON ar.news_id = n.id
                ORDER BY ar.analyzed_at DESC
                LIMIT ?
                """,
                (limit,)
            )
            
            return [dict(row) for row in rows]
            
        except Exception as e:
            self.logger.error(f"获取分析日志失败: {e}")
            return []
