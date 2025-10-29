#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
新闻服务模块
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

from core.database import get_db_manager
from models.analysis import AnalysisResult
from models.news import News

logger = logging.getLogger(__name__)

class NewsService:
    """新闻服务类"""
    
    def __init__(self, config_manager=None):
        self.db_manager = get_db_manager()
        self.config_manager = config_manager
    
    def get_news_by_id(self, news_id: int) -> Optional[News]:
        """根据ID获取新闻"""
        try:
            row = self.db_manager.fetchone(
                """
                SELECT n.*, ar.is_black_swan, ar.surprise_score, ar.impact_score, 
                       ar.analysis_reason, ar.confidence, ar.analyzed_at
                FROM news n
                LEFT JOIN analysis_results ar ON n.id = ar.news_id
                WHERE n.id = ?
                """,
                (news_id,)
            )
            
            if not row:
                return None
            
            return self._row_to_news_with_analysis(row)
            
        except Exception as e:
            logger.error(f"获取新闻失败: {e}")
            return None
    
    def get_news_list(self, limit: int = 50, offset: int = 0, 
                     source: str = None, black_swan_only: bool = False) -> List[News]:
        """获取新闻列表"""
        try:
            # 构建查询条件
            conditions = []
            params = []
            
            if source:
                conditions.append("n.source_name = ?")
                params.append(source)
            
            if black_swan_only:
                conditions.append("ar.is_black_swan = 1")
            
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
            
            # 构建完整查询
            query = f"""
                SELECT n.*, ar.is_black_swan, ar.surprise_score, ar.impact_score, 
                       ar.analysis_reason, ar.confidence, ar.analyzed_at
                FROM news n
                LEFT JOIN analysis_results ar ON n.id = ar.news_id
                {where_clause}
                ORDER BY n.published_at DESC
                LIMIT ? OFFSET ?
            """
            
            params.extend([limit, offset])
            
            rows = self.db_manager.fetchall(query, tuple(params))
            
            news_list = []
            for row in rows:
                news = self._row_to_news_with_analysis(row)
                news_list.append(news)
            
            return news_list
            
        except Exception as e:
            logger.error(f"获取新闻列表失败: {e}")
            return []
    
    def search_news(self, keyword: str, limit: int = 50, offset: int = 0) -> List[News]:
        """搜索新闻"""
        try:
            query = """
                SELECT n.*, ar.is_black_swan, ar.surprise_score, ar.impact_score, 
                       ar.analysis_reason, ar.confidence, ar.analyzed_at
                FROM news n
                LEFT JOIN analysis_results ar ON n.id = ar.news_id
                WHERE n.title LIKE ? OR n.summary LIKE ? OR n.content LIKE ?
                ORDER BY n.published_at DESC
                LIMIT ? OFFSET ?
            """
            
            search_term = f"%{keyword}%"
            params = (search_term, search_term, search_term, limit, offset)
            
            rows = self.db_manager.fetchall(query, params)
            
            news_list = []
            for row in rows:
                news = self._row_to_news_with_analysis(row)
                news_list.append(news)
            
            return news_list
            
        except Exception as e:
            logger.error(f"搜索新闻失败: {e}")
            return []
    
    def get_black_swan_news(self, limit: int = 50, offset: int = 0) -> List[News]:
        """获取黑天鹅新闻"""
        return self.get_news_list(limit=limit, offset=offset, black_swan_only=True)
    
    def get_news_by_source(self, source: str, limit: int = 50, offset: int = 0) -> List[News]:
        """根据来源获取新闻"""
        return self.get_news_list(limit=limit, offset=offset, source=source)
    
    def get_news_count(self, source: str = None, black_swan_only: bool = False) -> int:
        """获取新闻总数"""
        try:
            conditions = []
            params = []
            
            if source:
                conditions.append("source_name = ?")
                params.append(source)
            
            if black_swan_only:
                conditions.append("id IN (SELECT news_id FROM analysis_results WHERE is_black_swan = 1)")
            
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
            
            query = f"SELECT COUNT(*) as count FROM news {where_clause}"
            
            row = self.db_manager.fetchone(query, tuple(params))
            return row['count'] if row else 0
            
        except Exception as e:
            logger.error(f"获取新闻总数失败: {e}")
            return 0
    
    def _row_to_news_with_analysis(self, row: Dict[str, Any]) -> News:
        """将数据库行转换为带分析结果的新闻对象"""
        # 创建新闻对象
        news = News()
        news.id = row['id']
        news.url = row['url']
        news.title = row['title']
        news.summary = row['summary']
        news.content = row['content']
        news.source_name = row['source_name']
        news.image_url = row['image_url']
        
        # 处理日期时间字段
        if row['published_at']:
            news.published_at = datetime.fromisoformat(row['published_at'])
        if row['created_at']:
            news.created_at = datetime.fromisoformat(row['created_at'])
        if row['updated_at']:
            news.updated_at = datetime.fromisoformat(row['updated_at'])
        
        # 如果有分析结果，创建分析结果对象
        if row['is_black_swan'] is not None:
            analysis = AnalysisResult()
            analysis.news_id = row['id']
            analysis.is_black_swan = bool(row['is_black_swan'])
            analysis.surprise_score = row['surprise_score'] or 0
            analysis.impact_score = row['impact_score'] or 0
            analysis.analysis_reason = row['analysis_reason']
            analysis.confidence = row['confidence'] or 0.0
            if row['analyzed_at']:
                analysis.analyzed_at = datetime.fromisoformat(row['analyzed_at'])
            
            news.analysis_result = analysis
        
        return news
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取新闻统计信息"""
        try:
            # 总新闻数
            total_news = self.get_news_count()
            
            # 黑天鹅新闻数
            black_swan_count = self.get_news_count(black_swan_only=True)
            
            # 按来源统计
            source_stats = self.db_manager.fetchall("""
                SELECT source_name, COUNT(*) as count
                FROM news
                GROUP BY source_name
                ORDER BY count DESC
            """)
            
            # 最新新闻时间
            latest_news = self.db_manager.fetchone("""
                SELECT MAX(published_at) as latest_time
                FROM news
            """)
            
            return {
                'total_news': total_news,
                'black_swan_news': black_swan_count,
                'black_swan_ratio': round(black_swan_count / total_news * 100, 2) if total_news > 0 else 0,
                'sources': [{'name': row['source_name'], 'count': row['count']} for row in source_stats],
                'latest_news_time': datetime.fromisoformat(latest_news['latest_time']) if latest_news and latest_news['latest_time'] else None
            }
            
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {
                'total_news': 0,
                'black_swan_news': 0,
                'black_swan_ratio': 0,
                'sources': [],
                'latest_news_time': None
            }
    
    def get_system_stats(self) -> Dict[str, Any]:
        """获取系统统计信息（兼容性方法）"""
        return self.get_statistics()
    
    def _news_to_dict(self, news: News) -> Dict[str, Any]:
        """将新闻对象转换为字典以便JSON序列化"""
        news_dict = news.to_dict()
        
        # 添加分析结果信息
        if news.analysis_result:
            news_dict['analysis'] = {
                'is_black_swan': news.analysis_result.is_black_swan,
                'surprise_score': news.analysis_result.surprise_score,
                'impact_score': news.analysis_result.impact_score,
                'analysis_reason': news.analysis_result.analysis_reason,
                'confidence': news.analysis_result.confidence,
                'analyzed_at': news.analysis_result.analyzed_at.isoformat() if news.analysis_result.analyzed_at else None
            }
        else:
            news_dict['analysis'] = None
            
        return news_dict
    
    def get_news_paginated(self, page: int = 1, per_page: int = 20,
                         filters: Dict[str, Any] = None, sort_by: str = 'date_desc') -> tuple:
        """获取分页新闻列表"""
        try:
            # 构建查询条件
            conditions = []
            params = []
            
            if filters:
                if filters.get('search'):
                    conditions.append("(n.title LIKE ? OR n.summary LIKE ? OR n.content LIKE ?)")
                    search_term = f"%{filters['search']}%"
                    params.extend([search_term, search_term, search_term])
                
                if filters.get('black_swan_only'):
                    conditions.append("ar.is_black_swan = 1")
                
                if filters.get('event_type'):
                    conditions.append("ar.event_type = ?")
                    params.append(filters['event_type'])
                
                if filters.get('risk_level'):
                    conditions.append("ar.risk_level = ?")
                    params.append(filters['risk_level'])
                
                if filters.get('time_range'):
                    # 这里需要根据time_range计算时间范围
                    # 暂时不实现，保持简单
                    pass
                
                if filters.get('source'):
                    conditions.append("n.source_name = ?")
                    params.append(filters['source'])
            
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
            
            # 构建排序
            order_clause = "ORDER BY n.published_at DESC"
            if sort_by == 'date_asc':
                order_clause = "ORDER BY n.published_at ASC"
            elif sort_by == 'title_asc':
                order_clause = "ORDER BY n.title ASC"
            elif sort_by == 'title_desc':
                order_clause = "ORDER BY n.title DESC"
            
            # 计算偏移量
            offset = (page - 1) * per_page
            
            # 获取数据
            query = f"""
                SELECT n.*, ar.is_black_swan, ar.surprise_score, ar.impact_score,
                       ar.analysis_reason, ar.confidence, ar.analyzed_at
                FROM news n
                LEFT JOIN analysis_results ar ON n.id = ar.news_id
                {where_clause}
                {order_clause}
                LIMIT ? OFFSET ?
            """
            
            params.extend([per_page, offset])
            logger.info(f"query:", {query})
            rows = self.db_manager.fetchall(query, tuple(params))
            
            news_list = []
            for row in rows:
                news = self._row_to_news_with_analysis(row)
                news_dict = self._news_to_dict(news)
                news_list.append(news_dict)
            
            # 获取总数
            count_query = f"""
                SELECT COUNT(*) as count
                FROM news n
                LEFT JOIN analysis_results ar ON n.id = ar.news_id
                {where_clause}
            """
            
            # 移除LIMIT和OFFSET参数来获取总数
            count_params = params[:-2] if len(params) > 2 else []
            count_row = self.db_manager.fetchone(count_query, tuple(count_params))
            total_count = count_row['count'] if count_row else 0
            
            return news_list, total_count
            
        except Exception as e:
            logger.error(f"获取分页新闻失败: {e}")
            return [], 0