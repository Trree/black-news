#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS解析器模块
"""

import asyncio
import hashlib
import logging
import re
import time
from collections import deque
from datetime import datetime, timedelta
from functools import wraps
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, urljoin, parse_qs, urlencode, urlunparse

import feedparser
import requests

from core.database import get_db_manager
from models.rss_source import RSSSource
from core.rss_source_manager import get_enabled_rss_sources
logger = logging.getLogger(__name__)

class RSSError(Exception):
    """RSS相关错误基类"""
    pass

class RSSFetchError(RSSError):
    """RSS拉取错误"""
    pass

class RSSParseError(RSSError):
    """RSS解析错误"""
    pass

class RSSDuplicateError(RSSError):
    """重复新闻错误"""
    pass

def retry_on_failure(max_retries=3, delay=1, backoff=2, exceptions=(Exception,)):
    """重试装饰器"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(f"操作失败，{current_delay}秒后重试 ({attempt + 1}/{max_retries}): {str(e)}")
                        await asyncio.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"操作失败，已达到最大重试次数: {str(e)}")
            
            raise last_exception
        return wrapper
    return decorator

class RateLimiter:
    """速率限制器"""
    
    def __init__(self, max_requests: int, time_window: int):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
    
    async def acquire(self):
        """获取许可"""
        now = time.time()
        
        # 移除过期的请求记录
        while self.requests and self.requests[0] <= now - self.time_window:
            self.requests.popleft()
        
        # 检查是否超过限制
        if len(self.requests) >= self.max_requests:
            sleep_time = self.requests[0] + self.time_window - now
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
                # 递归调用，确保在等待后再次检查
                return await self.acquire()
        
        self.requests.append(now)

class DeduplicationManager:
    """去重管理器 - 基于URL的新闻去重"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
    
    async def is_duplicate(self, url: str) -> bool:
        """检查URL是否重复"""
        if not url:
            return True
        
        # 规范化URL
        normalized_url = self._normalize_url(url)
        
        # 检查数据库
        return await self._check_database_duplicate(normalized_url)
    
    def _normalize_url(self, url: str) -> str:
        """规范化URL"""
        try:
            parsed = urlparse(url)
            
            # 移除常见的跟踪参数
            query_params = parse_qs(parsed.query)
            filtered_params = {}
            
            for key, values in query_params.items():
                # 保留重要参数，移除跟踪参数
                if not self._is_tracking_param(key):
                    filtered_params[key] = values
            
            # 重建URL
            normalized_query = urlencode(filtered_params, doseq=True)
            normalized_url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                normalized_query,
                parsed.fragment
            ))
            
            return normalized_url
            
        except Exception as e:
            self.logger.warning(f"URL规范化失败 {url}: {str(e)}")
            return url
    
    def _is_tracking_param(self, param_name: str) -> bool:
        """判断是否为跟踪参数"""
        tracking_params = {
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'fbclid', 'gclid', 'msclkid', 'trk', 'tracking', 'ref'
        }
        return param_name.lower() in tracking_params
    
    async def _check_database_duplicate(self, url: str) -> bool:
        """检查数据库中的重复URL"""
        try:
            # 使用URL哈希进行快速查找
            url_hash = hashlib.md5(url.encode()).hexdigest()
            
            result = self.db_manager.fetchone(
                "SELECT id FROM news WHERE url = ?",
                (url,)
            )
            
            return result is not None
            
        except Exception as e:
            self.logger.error(f"数据库去重检查失败: {str(e)}")
            return False

class NewsValidator:
    """新闻数据验证器"""
    
    @staticmethod
    def validate_news_data(news_data: Dict[str, Any]) -> None:
        """验证新闻数据"""
        errors = []
        
        # 验证必填字段
        if not news_data.get('url'):
            errors.append("URL不能为空")
        
        if not news_data.get('title'):
            errors.append("标题不能为空")
        
        # 验证URL格式
        url = news_data.get('url', '')
        if not NewsValidator._is_valid_url(url):
            errors.append(f"无效的URL格式: {url}")
        
        # 验证发布时间
        published_at = news_data.get('published_at')
        if published_at and published_at > datetime.utcnow() + timedelta(days=1):
            errors.append("发布时间不能在未来")
        
        if errors:
            raise ValueError("; ".join(errors))
    
    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """验证URL格式"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    @staticmethod
    def sanitize_news_data(news_data: Dict[str, Any]) -> Dict[str, Any]:
        """清理新闻数据"""
        sanitized = news_data.copy()
        
        # 清理文本字段
        text_fields = ['title', 'summary', 'content']
        for field in text_fields:
            if field in sanitized and sanitized[field]:
                sanitized[field] = sanitized[field].strip()
                # 移除过多的空白字符
                sanitized[field] = re.sub(r'\s+', ' ', sanitized[field])
        
        # 截断过长的内容
        if 'content' in sanitized and sanitized['content']:
            max_length = 10000  # 最大内容长度
            if len(sanitized['content']) > max_length:
                sanitized['content'] = sanitized['content'][:max_length] + '...'
        
        return sanitized

class RSSMonitor:
    """RSS拉取监控"""
    
    def __init__(self):
        self.metrics = {
            'total_feeds': 0,
            'successful_feeds': 0,
            'failed_feeds': 0,
            'total_news': 0,
            'new_news': 0,
            'last_fetch_time': None
        }
    
    def record_fetch_result(self, result: Dict[str, Any]) -> None:
        """记录拉取结果"""
        self.metrics['total_feeds'] += 1
        
        if result['success']:
            self.metrics['successful_feeds'] += 1
            self.metrics['total_news'] += result.get('total_items', 0)
            self.metrics['new_news'] += result.get('new_items', 0)
        else:
            self.metrics['failed_feeds'] += 1
        
        self.metrics['last_fetch_time'] = datetime.utcnow()
    
    def get_metrics(self) -> Dict[str, Any]:
        """获取监控指标"""
        return self.metrics.copy()
    
    def get_health_status(self) -> str:
        """获取健康状态"""
        if self.metrics['total_feeds'] == 0:
            return 'unknown'
        
        success_rate = self.metrics['successful_feeds'] / self.metrics['total_feeds']
        
        if success_rate >= 0.8:
            return 'healthy'
        elif success_rate >= 0.5:
            return 'degraded'
        else:
            return 'unhealthy'

class RSSParser:
    """RSS解析器 - 负责RSS源的拉取、解析和数据标准化"""
    
    def __init__(self, config_manager=None):
        self.config_manager = config_manager
        self.db_manager = get_db_manager()
        self.session = self._create_session()
        self.logger = logging.getLogger(__name__)
        self.dedup_manager = DeduplicationManager(self.db_manager)
        self.monitor = RSSMonitor()
    
    def _create_session(self) -> requests.Session:
        """创建HTTP会话"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'BlackSwanNewsBot/1.0 (+https://github.com/blackswan-news)',
            'Accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml'
        })
        return session
    
    async def fetch_all_feeds(self) -> List[Dict[str, Any]]:
        """异步拉取所有启用的RSS源"""
        # 从RSS源管理器获取启用的RSS源
        try:
            # 导入RSS源管理器
            enabled_sources = get_enabled_rss_sources()
            
            if not enabled_sources:
                self.logger.warning("没有找到启用的RSS源")
                return []
                
        except Exception as e:
            self.logger.error(f"获取RSS源列表失败: {str(e)}")
            return []
        
        tasks = []
        for source in enabled_sources:
            task = asyncio.create_task(self._fetch_single_feed(source))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    
    @retry_on_failure(max_retries=3, delay=2, exceptions=(RSSFetchError, RSSParseError))
    async def _fetch_single_feed(self, source: RSSSource) -> Dict[str, Any]:
        """拉取单个RSS源"""
        try:
            self.logger.info(f"开始拉取RSS源: {source.name} - {source.url}")
            
            # 1. 发送HTTP请求
            response = await self._make_request(source.url)
            
            # 2. 解析Feed
            feed_data = self._parse_feed(response.content, source.url)
            
            # 3. 处理新闻条目
            new_news_count = await self._process_feed_entries(feed_data.entries, source)
            
            # 4. 更新拉取状态
            self._update_fetch_status(source.id, True, new_news_count)
            
            self.logger.info(f"成功拉取RSS源 {source.name}, 新增 {new_news_count} 条新闻")
            result = {
                'source': source.name,
                'success': True,
                'new_items': new_news_count,
                'total_items': len(feed_data.entries)
            }
            
            # 记录监控数据
            self.monitor.record_fetch_result(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"拉取RSS源失败 {source.name}: {str(e)}")
            self._update_fetch_status(source.id, False, 0, str(e))
            result = {
                'source': source.name,
                'success': False,
                'error': str(e)
            }
            
            # 记录监控数据
            self.monitor.record_fetch_result(result)
            
            return result
    
    async def _make_request(self, url: str, timeout: int = 30) -> requests.Response:
        """发送HTTP请求"""
        try:
            response = self.session.get(
                url,
                timeout=timeout,
                allow_redirects=True
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            raise RSSFetchError(f"HTTP请求失败: {str(e)}")
    
    def _parse_feed(self, content: bytes, url: str) -> feedparser.FeedParserDict:
        """解析RSS/Atom Feed"""
        try:
            # 使用feedparser解析
            feed = feedparser.parse(content)
            
            if feed.bozo:
                # 处理解析错误
                bozo_exception = feed.bozo_exception
                self.logger.warning(f"Feed解析警告: {bozo_exception}")
            
            if not feed.entries:
                raise RSSParseError("Feed中没有找到新闻条目")
            
            return feed
            
        except Exception as e:
            raise RSSParseError(f"Feed解析失败: {str(e)}")
    
    async def _process_feed_entries(self, entries: List[Any], source: RSSSource) -> int:
        """处理Feed中的新闻条目"""
        new_news_count = 0
        
        for entry in entries:
            try:
                # 1. 标准化新闻数据
                news_data = self._standardize_news_entry(entry, source)
                
                # 2. 验证数据
                try:
                    NewsValidator.validate_news_data(news_data)
                except ValueError as e:
                    self.logger.warning(f"新闻数据验证失败: {str(e)}")
                    continue
                
                # 3. 清理数据
                news_data = NewsValidator.sanitize_news_data(news_data)
                
                # 4. 去重检查
                if await self.dedup_manager.is_duplicate(news_data['url']):
                    continue
                
                # 5. 保存新闻
                news_id = await self._save_news(news_data)
                
                # 6. 触发分析
                if news_id:
                    # 这里应该触发分析任务，但暂时留空
                    new_news_count += 1
                    
            except Exception as e:
                self.logger.error(f"处理新闻条目失败: {str(e)}")
                continue
        
        return new_news_count
    
    def _standardize_news_entry(self, entry: Any, source: RSSSource) -> Dict[str, Any]:
        """标准化新闻数据"""
        # 提取标题
        title = self._extract_title(entry)
        
        # 提取摘要
        summary = self._extract_summary(entry)
        
        # 提取内容
        content = self._extract_content(entry)
        
        # 提取发布时间
        published_at = self._extract_published_date(entry)
        
        # 提取图片
        image_url = self._extract_image(entry)
        
        # 构建URL（处理相对URL）
        url = self._build_absolute_url(entry.link, source.url)
        
        return {
            'url': url,
            'title': title,
            'summary': summary,
            'content': content,
            'source_name': source.name,
            'published_at': published_at,
            'image_url': image_url,
            'source_id': source.id
        }
    
    def _extract_title(self, entry: Any) -> str:
        """提取标题"""
        return getattr(entry, 'title', '').strip()
    
    def _extract_summary(self, entry: Any) -> str:
        """提取摘要"""
        summary = getattr(entry, 'summary', '')
        if not summary:
            summary = getattr(entry, 'description', '')
        
        # 清理HTML标签
        summary = self._clean_html(summary)
        return summary.strip()
    
    def _extract_content(self, entry: Any) -> str:
        """提取完整内容"""
        # 优先使用content字段
        if hasattr(entry, 'content') and entry.content:
            content = entry.content[0].value
        else:
            content = getattr(entry, 'summary', '')
            if not content:
                content = getattr(entry, 'description', '')
        
        # 清理HTML标签但保留文本内容
        content = self._clean_html(content)
        return content.strip()
    
    def _extract_published_date(self, entry: Any) -> datetime:
        """提取发布时间"""
        date_fields = ['published_parsed', 'updated_parsed', 'created_parsed']
        
        for field in date_fields:
            if hasattr(entry, field) and getattr(entry, field):
                date_tuple = getattr(entry, field)
                try:
                    return datetime(*date_tuple[:6])
                except (ValueError, TypeError):
                    continue
        
        # 如果没有解析的时间，使用当前时间
        return datetime.utcnow()
    
    def _extract_image(self, entry: Any) -> Optional[str]:
        """提取图片URL"""
        # 检查media:content
        if hasattr(entry, 'media_content') and entry.media_content:
            for media in entry.media_content:
                if media.get('type', '').startswith('image/'):
                    return media.get('url')
        
        # 检查enclosures
        if hasattr(entry, 'enclosures') and entry.enclosures:
            for enclosure in entry.enclosures:
                if enclosure.get('type', '').startswith('image/'):
                    return enclosure.get('href')
        
        # 从内容中提取图片
        content = getattr(entry, 'content', [{}])[0].get('value', '') if hasattr(entry, 'content') else ''
        content += getattr(entry, 'summary', '')
        
        # 使用正则表达式提取img标签
        img_pattern = r'<img[^>]+src="([^">]+)"'
        matches = re.findall(img_pattern, content)
        if matches:
            return matches[0]
        
        return None
    
    def _build_absolute_url(self, url: str, base_url: str) -> str:
        """构建绝对URL"""
        if not url:
            return None
        
        if url.startswith(('http://', 'https://')):
            return url
        
        # 处理相对URL
        try:
            return urljoin(base_url, url)
        except Exception:
            return url
    
    def _clean_html(self, html_text: str) -> str:
        """清理HTML标签"""
        if not html_text:
            return ''
        
        # 简单的正则清理
        clean_text = re.sub(r'<[^>]+>', '', html_text)
        return clean_text.strip()
    
    async def _save_news(self, news_data: Dict[str, Any]) -> Optional[int]:
        """保存新闻到数据库"""
        try:
            cursor = self.db_manager.execute(
                """
                INSERT OR IGNORE INTO news 
                (url, title, summary, content, source_name, published_at, image_url, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    news_data['url'],
                    news_data['title'],
                    news_data['summary'],
                    news_data['content'],
                    news_data['source_name'],
                    news_data['published_at'],
                    news_data['image_url'],
                    datetime.utcnow(),
                    datetime.utcnow()
                )
            )
            
            # 如果插入成功，返回新闻ID
            if cursor.rowcount > 0:
                # 获取插入的新闻ID
                result = self.db_manager.fetchone(
                    "SELECT id FROM news WHERE url = ?",
                    (news_data['url'],)
                )
                return result['id'] if result else None
            else:
                # 新闻已存在
                return None
                
        except Exception as e:
            self.logger.error(f"保存新闻失败: {str(e)}")
            return None
    
    def _update_fetch_status(self, source_id: int, success: bool, items_fetched: int = 0, error_message: str = None) -> None:
        """更新拉取状态"""
        try:
            # 更新RSS源的最后拉取时间
            self.db_manager.execute(
                "UPDATE rss_sources SET last_fetched = ? WHERE id = ?",
                (datetime.utcnow(), source_id)
            )
            
            # 记录拉取日志
            self.db_manager.execute(
                """
                INSERT INTO fetch_logs 
                (rss_source_id, success, items_fetched, error_message, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (source_id, success, items_fetched, error_message, datetime.utcnow())
            )
            
        except Exception as e:
            self.logger.error(f"更新拉取状态失败: {str(e)}")
    
    def get_monitor_metrics(self) -> Dict[str, Any]:
        """获取监控指标"""
        return self.monitor.get_metrics()
    
    def get_health_status(self) -> str:
        """获取健康状态"""
        return self.monitor.get_health_status()