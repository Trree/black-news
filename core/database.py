#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库操作模块
"""

import logging
import sqlite3
from contextlib import contextmanager
from typing import Optional, List

# 配置日志
logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, db_path: str = "black_swan_news.db"):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """初始化数据库"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # 启用外键约束
                cursor.execute("PRAGMA foreign_keys = ON")
                
                # 创建新闻表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS news (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT UNIQUE NOT NULL,
                        title TEXT NOT NULL,
                        summary TEXT,
                        content TEXT,
                        source_name TEXT,
                        published_at DATETIME,
                        image_url TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建分析结果表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS analysis_results (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        news_id INTEGER NOT NULL,
                        is_black_swan BOOLEAN NOT NULL,
                        surprise_score INTEGER CHECK (surprise_score >= 1 AND surprise_score <= 10),
                        impact_score INTEGER CHECK (impact_score >= 1 AND impact_score <= 10),
                        analysis_reason TEXT,
                        confidence FLOAT CHECK (confidence >= 0 AND confidence <= 1),
                        analyzed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (news_id) REFERENCES news (id) ON DELETE CASCADE
                    )
                """)
                
                # 创建配置表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS config (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        description TEXT,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建RSS源表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS rss_sources (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        url TEXT UNIQUE NOT NULL,
                        enabled BOOLEAN DEFAULT true,
                        update_interval INTEGER DEFAULT 3600,
                        last_fetched DATETIME,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # 创建拉取日志表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS fetch_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        rss_source_id INTEGER NOT NULL,
                        success BOOLEAN NOT NULL,
                        items_fetched INTEGER DEFAULT 0,
                        error_message TEXT,
                        fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (rss_source_id) REFERENCES rss_sources (id) ON DELETE CASCADE
                    )
                """)
                
                # 创建索引
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_published_at ON news(published_at)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_news_source ON news(source_name)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_analysis_news_id ON analysis_results(news_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_analysis_black_swan ON analysis_results(is_black_swan)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_analysis_scores ON analysis_results(surprise_score, impact_score)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_rss_enabled ON rss_sources(enabled)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_source ON fetch_logs(rss_source_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_time ON fetch_logs(fetched_at)")
                
                # 插入默认配置
                cursor.execute("""
                    INSERT OR IGNORE INTO config (key, value, description) VALUES 
                    ('llm_api_base', 'https://api.openai.com/v1', 'LiteLLM API基础URL'),
                    ('llm_model', 'gpt-3.5-turbo', '使用的LLM模型'),
                    ('rss_fetch_interval', '3600', 'RSS拉取间隔(秒)'),
                    ('analysis_batch_size', '10', '批量分析数量'),
                    ('max_retries', '3', '最大重试次数')
                """)
                
                conn.commit()
                logger.info("数据库初始化完成")
                
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # 使结果可以通过列名访问
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute(self, query: str, params: tuple = ()) -> sqlite3.Cursor:
        """执行SQL查询"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
            return cursor
    
    def fetchone(self, query: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        """获取单条记录"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchone()
    
    def fetchall(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        """获取所有记录"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            return cursor.fetchall()

# 全局数据库管理器实例
db_manager = DatabaseManager()

# 便捷函数
def get_db_manager() -> DatabaseManager:
    """获取数据库管理器实例"""
    return db_manager

def init_database(db_path: str = "black_swan_news.db") -> DatabaseManager:
    """初始化数据库"""
    global db_manager
    db_manager = DatabaseManager(db_path)
    return db_manager