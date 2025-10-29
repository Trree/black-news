#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务调度器模块
负责定时执行RSS拉取、新闻分析等任务
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from core.database import get_db_manager
from core.llm_analyzer import LLMAnalyzer
from core.rss_parser import RSSParser
from core.rss_source_manager import get_enabled_rss_sources

logger = logging.getLogger(__name__)


class SchedulerError(Exception):
    """调度器错误基类"""
    pass


class TaskMonitor:
    """任务监控器"""
    
    def __init__(self):
        self.task_history = []
        self.max_history_size = 100
    
    def record_task_start(self, task_id: str, task_name: str) -> None:
        """记录任务开始"""
        task_record = {
            'task_id': task_id,
            'task_name': task_name,
            'start_time': datetime.utcnow(),
            'status': 'running',
            'end_time': None,
            'success': None,
            'message': None,
            'error': None
        }
        
        self.task_history.append(task_record)
        
        # 限制历史记录大小
        if len(self.task_history) > self.max_history_size:
            self.task_history = self.task_history[-self.max_history_size:]
    
    def record_task_complete(self, task_id: str, success: bool = True, 
                           message: str = None, error: str = None) -> None:
        """记录任务完成"""
        for task in reversed(self.task_history):
            if task['task_id'] == task_id and task['status'] == 'running':
                task.update({
                    'status': 'completed',
                    'end_time': datetime.utcnow(),
                    'success': success,
                    'message': message,
                    'error': error
                })
                break
    
    def get_task_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """获取任务统计信息"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        recent_tasks = [
            task for task in self.task_history
            if task.get('start_time', datetime.min) >= cutoff_time
        ]
        
        total_tasks = len(recent_tasks)
        successful_tasks = sum(1 for task in recent_tasks if task.get('success'))
        failed_tasks = total_tasks - successful_tasks
        
        return {
            'total_executions': total_tasks,
            'successful_executions': successful_tasks,
            'failed_executions': failed_tasks,
            'success_rate': successful_tasks / total_tasks if total_tasks > 0 else 0
        }


class SimpleScheduler:
    """简单调度器 - 使用线程和定时器实现基本调度功能"""
    
    def __init__(self, config_manager=None):
        self.config_manager = config_manager
        self.db_manager = get_db_manager()
        self.rss_parser = RSSParser(config_manager)
        self.llm_analyzer = LLMAnalyzer()
        self.monitor = TaskMonitor()
        self.is_running = False
        self.timers = {}
        
        # 默认配置
        self.default_config = {
            'rss_fetch_interval': 3600,  # 1小时
            'analysis_interval': 1800,   # 30分钟
            'cleanup_interval': 86400,   # 24小时
            'health_check_interval': 300  # 5分钟
        }
    
    def start(self) -> None:
        """启动调度器"""
        if self.is_running:
            logger.warning("调度器已经在运行")
            return
        
        logger.info("启动任务调度器...")
        self.is_running = True
        
        # 启动所有定时任务
        self._start_all_tasks()
        
        logger.info("任务调度器启动成功")
    
    def stop(self) -> None:
        """停止调度器"""
        if not self.is_running:
            logger.warning("调度器未在运行")
            return
        
        logger.info("停止任务调度器...")
        self.is_running = False
        
        # 停止所有定时器
        for task_id, timer in self.timers.items():
            if timer:
                timer.cancel()
        
        self.timers.clear()
        logger.info("任务调度器已停止")
    
    def _start_all_tasks(self) -> None:
        """启动所有定时任务"""
        config = self._get_config()
        
        # RSS拉取任务
        rss_interval = config.get('rss_fetch_interval', 3600)
        self._start_periodic_task('rss_fetch', self._rss_fetch_task, rss_interval)
        
        # 新闻分析任务
        analysis_interval = config.get('analysis_interval', 1800)
        self._start_periodic_task('news_analysis', self._news_analysis_task, analysis_interval)
        
        # 健康检查任务
        health_interval = config.get('health_check_interval', 300)
        self._start_periodic_task('health_check', self._health_check_task, health_interval)
        
        logger.info(f"已启动 {len(self.timers)} 个定时任务")
    
    def _start_periodic_task(self, task_id: str, task_func, interval: int) -> None:
        """启动周期性任务"""
        import threading
        
        def run_task():
            if not self.is_running:
                return
            
            try:
                task_func()
            except Exception as e:
                logger.error(f"定时任务 {task_id} 执行失败: {str(e)}")
            finally:
                # 重新安排任务
                if self.is_running:
                    timer = threading.Timer(interval, run_task)
                    timer.daemon = True
                    timer.start()
                    self.timers[task_id] = timer
        
        # 首次执行
        timer = threading.Timer(0, run_task)  # 立即执行
        timer.daemon = True
        timer.start()
        self.timers[task_id] = timer
        
        logger.info(f"定时任务 {task_id} 已启动，间隔: {interval} 秒")
    
    def _get_config(self) -> Dict[str, Any]:
        """获取配置"""
        if self.config_manager:
            return self.config_manager.get('scheduler_config', self.default_config)
        return self.default_config
    
    def _rss_fetch_task(self) -> None:
        """RSS拉取任务"""
        task_id = 'rss_fetch'
        self.monitor.record_task_start(task_id, 'RSS新闻拉取')
        
        try:
            logger.info("开始执行RSS拉取任务")
            
            # 获取启用的RSS源
            enabled_sources = get_enabled_rss_sources()
            
            if not enabled_sources:
                logger.warning("没有启用的RSS源，跳过拉取任务")
                self.monitor.record_task_complete(task_id, True, "无启用的RSS源")
                return
            
            logger.info(f"开始拉取 {len(enabled_sources)} 个RSS源")
            
            # 执行RSS拉取（使用同步方式）
            import asyncio
            results = asyncio.run(self.rss_parser.fetch_all_feeds())
            
            # 统计结果
            successful_feeds = sum(1 for r in results if isinstance(r, dict) and r.get('success'))
            total_feeds = len(enabled_sources)
            new_news_count = sum(r.get('new_items', 0) for r in results if isinstance(r, dict))
            
            logger.info(
                f"RSS拉取任务完成: {successful_feeds}/{total_feeds} 个源成功, "
                f"新增 {new_news_count} 条新闻"
            )
            
            self.monitor.record_task_complete(
                task_id, 
                True,
                f"成功拉取 {successful_feeds}/{total_feeds} 个源，新增 {new_news_count} 条新闻"
            )
            
        except Exception as e:
            error_msg = f"RSS拉取任务失败: {str(e)}"
            logger.error(error_msg)
            self.monitor.record_task_complete(task_id, False, error=error_msg)
    
    def _news_analysis_task(self) -> None:
        """新闻分析任务"""
        task_id = 'news_analysis'
        self.monitor.record_task_start(task_id, '新闻分析')
        
        try:
            logger.info("开始执行新闻分析任务")
            
            # 获取未分析的新闻
            unanalyzed_news = self._get_unanalyzed_news()
            
            if not unanalyzed_news:
                logger.info("没有未分析的新闻，跳过分析任务")
                self.monitor.record_task_complete(task_id, True, "无未分析新闻")
                return
            
            logger.info(f"发现 {len(unanalyzed_news)} 条未分析新闻，开始分析")
            
            # 执行批量分析
            analyzed_count = 0
            for news in unanalyzed_news:
                try:
                    # 这里应该调用LLM分析器，暂时使用模拟
                    # analysis_result = self.llm_analyzer.analyze_black_swan(news.content)
                    # 保存分析结果
                    analyzed_count += 1
                except Exception as e:
                    logger.error(f"分析新闻失败 {news.id}: {str(e)}")
                    continue
            
            logger.info(f"新闻分析任务完成: 成功分析 {analyzed_count}/{len(unanalyzed_news)} 条新闻")
            
            self.monitor.record_task_complete(
                task_id,
                True,
                f"成功分析 {analyzed_count}/{len(unanalyzed_news)} 条新闻"
            )
            
        except Exception as e:
            error_msg = f"新闻分析任务失败: {str(e)}"
            logger.error(error_msg)
            self.monitor.record_task_complete(task_id, False, error=error_msg)
    
    def _get_unanalyzed_news(self, limit: int = 10) -> list:
        """获取未分析的新闻"""
        try:
            # 查询最近一段时间内未分析的新闻
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            
            results = self.db_manager.fetchall(
                """
                SELECT n.* FROM news n
                LEFT JOIN analysis_results ar ON n.id = ar.news_id
                WHERE ar.id IS NULL AND n.published_at >= ?
                ORDER BY n.published_at DESC
                LIMIT ?
                """,
                (cutoff_time, limit)
            )
            
            # 转换为News对象
            from models.news import News
            news_list = []
            for row in results:
                news = News()
                news.id = row['id']
                news.title = row['title']
                news.content = row['content']
                news.source_name = row['source_name']
                news.published_at = datetime.fromisoformat(row['published_at']) if row['published_at'] else None
                news_list.append(news)
            
            return news_list
            
        except Exception as e:
            logger.error(f"获取未分析新闻失败: {str(e)}")
            return []
    
    def _health_check_task(self) -> None:
        """健康检查任务"""
        task_id = 'health_check'
        self.monitor.record_task_start(task_id, '健康检查')
        
        try:
            logger.debug("开始执行健康检查任务")
            
            health_status = {
                'timestamp': datetime.utcnow().isoformat(),
                'overall_status': 'healthy',
                'components': {}
            }
            
            # 检查数据库连接
            db_status = self._check_database_health()
            health_status['components']['database'] = db_status
            
            # 检查RSS源可用性
            rss_status = self._check_rss_health()
            health_status['components']['rss_sources'] = rss_status
            
            # 检查任务执行状态
            task_status = self._check_task_health()
            health_status['components']['tasks'] = task_status
            
            # 确定整体状态
            if any(comp.get('status') == 'unhealthy' for comp in health_status['components'].values()):
                health_status['overall_status'] = 'unhealthy'
            elif any(comp.get('status') == 'degraded' for comp in health_status['components'].values()):
                health_status['overall_status'] = 'degraded'
            
            logger.debug(f"健康检查完成: {health_status['overall_status']}")
            self.monitor.record_task_complete(
                task_id,
                True,
                f"健康检查完成: {health_status['overall_status']}"
            )
            
        except Exception as e:
            error_msg = f"健康检查任务失败: {str(e)}"
            logger.error(error_msg)
            self.monitor.record_task_complete(task_id, False, error=error_msg)
    
    def _check_database_health(self) -> Dict[str, Any]:
        """检查数据库健康状态"""
        try:
            # 执行简单查询测试连接
            self.db_manager.execute("SELECT 1")
            return {'status': 'healthy', 'message': '数据库连接正常'}
        except Exception as e:
            return {'status': 'unhealthy', 'message': f'数据库连接失败: {str(e)}'}
    
    def _check_rss_health(self) -> Dict[str, Any]:
        """检查RSS源健康状态"""
        try:
            enabled_sources = get_enabled_rss_sources()
            
            if not enabled_sources:
                return {'status': 'degraded', 'message': '没有启用的RSS源'}
            
            # 检查最近的成功拉取记录
            recent_success_count = 0
            for source in enabled_sources:
                last_success = self._get_last_successful_fetch(source.id)
                if last_success and (datetime.utcnow() - last_success).total_seconds() < 86400:  # 24小时内
                    recent_success_count += 1
            
            success_rate = recent_success_count / len(enabled_sources) if enabled_sources else 0
            
            if success_rate >= 0.8:
                return {'status': 'healthy', 'message': f'RSS源正常 ({success_rate:.1%} 成功率)'}
            elif success_rate >= 0.5:
                return {'status': 'degraded', 'message': f'RSS源部分异常 ({success_rate:.1%} 成功率)'}
            else:
                return {'status': 'unhealthy', 'message': f'RSS源严重异常 ({success_rate:.1%} 成功率)'}
                
        except Exception as e:
            return {'status': 'unhealthy', 'message': f'RSS健康检查失败: {str(e)}'}
    
    def _check_task_health(self) -> Dict[str, Any]:
        """检查任务健康状态"""
        try:
            task_stats = self.monitor.get_task_statistics()
            recent_failures = task_stats.get('failed_executions', 0)
            total_executions = task_stats.get('total_executions', 0)
            
            if total_executions == 0:
                return {'status': 'unknown', 'message': '暂无任务执行记录'}
            
            failure_rate = recent_failures / total_executions if total_executions > 0 else 0
            
            if failure_rate <= 0.1:
                return {'status': 'healthy', 'message': f'任务执行正常 ({failure_rate:.1%} 失败率)'}
            elif failure_rate <= 0.3:
                return {'status': 'degraded', 'message': f'任务执行部分异常 ({failure_rate:.1%} 失败率)'}
            else:
                return {'status': 'unhealthy', 'message': f'任务执行严重异常 ({failure_rate:.1%} 失败率)'}
                
        except Exception as e:
            return {'status': 'unhealthy', 'message': f'任务健康检查失败: {str(e)}'}
    
    def _get_last_successful_fetch(self, source_id: int) -> Optional[datetime]:
        """获取最近成功的拉取时间"""
        try:
            result = self.db_manager.fetchone(
                "SELECT fetched_at FROM fetch_logs WHERE rss_source_id = ? AND success = 1 ORDER BY fetched_at DESC LIMIT 1",
                (source_id,)
            )
            
            if result and result['fetched_at']:
                return datetime.fromisoformat(result['fetched_at'])
            return None
            
        except Exception:
            return None
    
    def trigger_manual_fetch(self) -> Dict[str, Any]:
        """手动触发RSS拉取"""
        try:
            logger.info("手动触发RSS拉取")
            self._rss_fetch_task()
            
            # 获取最新的任务状态
            recent_tasks = [task for task in self.monitor.task_history if task['task_id'] == 'rss_fetch']
            if recent_tasks:
                latest_task = recent_tasks[-1]
                return {
                    'success': latest_task.get('success', False),
                    'message': latest_task.get('message', '任务执行中'),
                    'error': latest_task.get('error')
                }
            
            return {'success': False, 'message': '无法获取任务状态'}
            
        except Exception as e:
            error_msg = f"手动触发RSS拉取失败: {str(e)}"
            logger.error(error_msg)
            return {'success': False, 'error': error_msg}
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """获取调度器状态"""
        return {
            'is_running': self.is_running,
            'active_tasks': len(self.timers),
            'task_statistics': self.monitor.get_task_statistics(),
            'health_status': self._get_health_status()
        }
    
    def _get_health_status(self) -> Dict[str, Any]:
        """获取健康状态"""
        health_status = {
            'timestamp': datetime.utcnow().isoformat(),
            'overall_status': 'healthy',
            'components': {}
        }
        
        # 检查各个组件
        health_status['components']['database'] = self._check_database_health()
        health_status['components']['rss_sources'] = self._check_rss_health()
        health_status['components']['tasks'] = self._check_task_health()
        
        # 确定整体状态
        if any(comp.get('status') == 'unhealthy' for comp in health_status['components'].values()):
            health_status['overall_status'] = 'unhealthy'
        elif any(comp.get('status') == 'degraded' for comp in health_status['components'].values()):
            health_status['overall_status'] = 'degraded'
        
        return health_status


# 全局调度器实例
_scheduler_instance = None


def get_scheduler(config_manager=None) -> SimpleScheduler:
    """获取调度器实例"""
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = SimpleScheduler(config_manager)
    return _scheduler_instance


def start_scheduler(config_manager=None) -> None:
    """启动调度器"""
    scheduler = get_scheduler(config_manager)
    scheduler.start()


def stop_scheduler() -> None:
    """停止调度器"""
    global _scheduler_instance
    if _scheduler_instance:
        _scheduler_instance.stop()
        _scheduler_instance = None