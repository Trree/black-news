"""
日志记录器模块
提供统一的日志记录功能
"""

import logging
import logging.handlers
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path


# 日志级别映射
LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


class StructuredLogger:
    """
    结构化日志记录器
    """
    
    def __init__(self, name: str = 'black_swan_news', log_level: str = 'INFO'):
        """
        初始化日志记录器
        
        Args:
            name: 日志记录器名称
            log_level: 日志级别
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(LOG_LEVELS.get(log_level, logging.INFO))
        
        # 避免重复添加处理器
        if not self.logger.handlers:
            self._setup_handlers()
    
    def _setup_handlers(self):
        """设置日志处理器"""
        # 创建日志目录
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # 文件处理器 - 所有日志
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir / 'black_swan_news.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # 错误日志文件处理器
        error_handler = logging.handlers.RotatingFileHandler(
            log_dir / 'errors.log',
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        self.logger.addHandler(error_handler)
        
        # JSON格式日志处理器
        json_handler = logging.handlers.RotatingFileHandler(
            log_dir / 'structured.log',
            maxBytes=10*1024*1024,
            backupCount=5
        )
        json_formatter = JSONFormatter()
        json_handler.setFormatter(json_formatter)
        self.logger.addHandler(json_handler)
    
    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """记录调试日志"""
        self.logger.debug(message, extra=extra or {})
    
    def info(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """记录信息日志"""
        self.logger.info(message, extra=extra or {})
    
    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """记录警告日志"""
        self.logger.warning(message, extra=extra or {})
    
    def error(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """记录错误日志"""
        self.logger.error(message, extra=extra or {})
    
    def critical(self, message: str, extra: Optional[Dict[str, Any]] = None):
        """记录严重错误日志"""
        self.logger.critical(message, extra=extra or {})
    
    def log_rss_fetch(self, source_name: str, success: bool, 
                     items_count: int = 0, error: Optional[str] = None):
        """
        记录RSS抓取日志
        
        Args:
            source_name: 数据源名称
            success: 是否成功
            items_count: 抓取到的项目数量
            error: 错误信息（如果有）
        """
        extra = {
            'source_name': source_name,
            'success': success,
            'items_count': items_count,
            'event_type': 'rss_fetch'
        }
        
        if success:
            message = f"RSS抓取成功: {source_name} - 获取 {items_count} 个项目"
            self.info(message, extra=extra)
        else:
            message = f"RSS抓取失败: {source_name} - {error}"
            extra['error'] = error
            self.error(message, extra=extra)
    
    def log_analysis(self, news_id: str, success: bool, 
                    is_black_swan: bool = False, 
                    confidence: float = 0.0,
                    error: Optional[str] = None):
        """
        记录分析日志
        
        Args:
            news_id: 新闻ID
            success: 是否成功
            is_black_swan: 是否为黑天鹅事件
            confidence: 置信度
            error: 错误信息（如果有）
        """
        extra = {
            'news_id': news_id,
            'success': success,
            'is_black_swan': is_black_swan,
            'confidence': confidence,
            'event_type': 'analysis'
        }
        
        if success:
            event_type = "黑天鹅事件" if is_black_swan else "常规事件"
            message = f"分析完成: {news_id} - {event_type} (置信度: {confidence:.2f})"
            self.info(message, extra=extra)
        else:
            message = f"分析失败: {news_id} - {error}"
            extra['error'] = error
            self.error(message, extra=extra)
    
    def log_system_event(self, event: str, details: Optional[Dict[str, Any]] = None):
        """
        记录系统事件
        
        Args:
            event: 事件描述
            details: 详细信息
        """
        extra = {
            'event_type': 'system',
            'details': details or {}
        }
        
        self.info(f"系统事件: {event}", extra=extra)
    
    def log_user_action(self, action: str, user_id: Optional[str] = None,
                       details: Optional[Dict[str, Any]] = None):
        """
        记录用户操作
        
        Args:
            action: 操作描述
            user_id: 用户ID（可选）
            details: 详细信息
        """
        extra = {
            'event_type': 'user_action',
            'user_id': user_id,
            'action': action,
            'details': details or {}
        }
        
        message = f"用户操作: {action}"
        if user_id:
            message += f" (用户: {user_id})"
        
        self.info(message, extra=extra)
    
    def log_performance(self, operation: str, duration: float, 
                       details: Optional[Dict[str, Any]] = None):
        """
        记录性能日志
        
        Args:
            operation: 操作名称
            duration: 耗时（秒）
            details: 详细信息
        """
        extra = {
            'event_type': 'performance',
            'operation': operation,
            'duration': duration,
            'details': details or {}
        }
        
        self.info(f"性能监控: {operation} - 耗时 {duration:.3f}秒", extra=extra)


class JSONFormatter(logging.Formatter):
    """JSON格式的日志格式化器"""
    
    def format(self, record: logging.LogRecord) -> str:
        """
        格式化日志记录为JSON
        
        Args:
            record: 日志记录
            
        Returns:
            JSON格式的日志字符串
        """
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # 添加额外字段
        if hasattr(record, 'event_type'):
            log_entry['event_type'] = record.event_type
        
        # 添加所有extra字段
        for key, value in record.__dict__.items():
            if key not in ['args', 'created', 'exc_info', 'exc_text', 'filename', 
                          'funcName', 'levelname', 'levelno', 'lineno', 'module', 
                          'msecs', 'message', 'msg', 'name', 'pathname', 'process', 
                          'processName', 'relativeCreated', 'stack_info', 'thread', 
                          'threadName', 'event_type']:
                log_entry[key] = value
        
        # 处理异常信息
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        return json.dumps(log_entry, ensure_ascii=False, default=str)


class DatabaseLogger:
    """
    数据库日志记录器（用于记录到数据库表）
    """
    
    def __init__(self, db_connection):
        """
        初始化数据库日志记录器
        
        Args:
            db_connection: 数据库连接
        """
        self.db = db_connection
    
    def log_event(self, event_type: str, message: str, 
                 details: Optional[Dict[str, Any]] = None,
                 user_id: Optional[str] = None,
                 source_id: Optional[str] = None,
                 news_id: Optional[str] = None):
        """
        记录事件到数据库
        
        Args:
            event_type: 事件类型
            message: 事件消息
            details: 详细信息
            user_id: 用户ID
            source_id: 数据源ID
            news_id: 新闻ID
        """
        try:
            cursor = self.db.cursor()
            cursor.execute('''
                INSERT INTO system_logs 
                (event_type, message, details, user_id, source_id, news_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                event_type,
                message,
                json.dumps(details) if details else None,
                user_id,
                source_id,
                news_id,
                datetime.now().isoformat()
            ))
            self.db.commit()
        except Exception as e:
            # 如果数据库日志失败，回退到文件日志
            fallback_logger = StructuredLogger('database_logger_fallback')
            fallback_logger.error(f"数据库日志记录失败: {str(e)}")


# 全局日志记录器实例
_logger_instance = None


def get_logger(name: str = 'black_swan_news', log_level: str = 'INFO') -> StructuredLogger:
    """
    获取日志记录器实例（单例模式）
    
    Args:
        name: 日志记录器名称
        log_level: 日志级别
        
    Returns:
        日志记录器实例
    """
    global _logger_instance
    
    if _logger_instance is None:
        _logger_instance = StructuredLogger(name, log_level)
    
    return _logger_instance


def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None):
    """
    设置全局日志配置
    
    Args:
        log_level: 日志级别
        log_file: 日志文件路径（可选）
    """
    logging.basicConfig(
        level=LOG_LEVELS.get(log_level, logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file) if log_file else logging.StreamHandler()
        ]
    )


def log_system_startup():
    """记录系统启动日志"""
    logger = get_logger()
    logger.log_system_event("系统启动", {
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0",
        "environment": os.getenv('ENVIRONMENT', 'development')
    })


def log_system_shutdown():
    """记录系统关闭日志"""
    logger = get_logger()
    logger.log_system_event("系统关闭", {
        "timestamp": datetime.now().isoformat()
    })


def log_config_change(config_key: str, old_value: Any, new_value: Any):
    """
    记录配置变更
    
    Args:
        config_key: 配置键
        old_value: 旧值
        new_value: 新值
    """
    logger = get_logger()
    logger.log_system_event("配置变更", {
        "config_key": config_key,
        "old_value": str(old_value),
        "new_value": str(new_value)
    })


def log_data_export(export_type: str, record_count: int, 
                   format_type: str, user_id: Optional[str] = None):
    """
    记录数据导出日志
    
    Args:
        export_type: 导出类型
        record_count: 记录数量
        format_type: 导出格式
        user_id: 用户ID
    """
    logger = get_logger()
    logger.log_user_action("数据导出", user_id, {
        "export_type": export_type,
        "record_count": record_count,
        "format_type": format_type
    })


def log_error_with_context(error: Exception, context: Dict[str, Any]):
    """
    记录带上下文的错误
    
    Args:
        error: 异常对象
        context: 上下文信息
    """
    logger = get_logger()
    logger.error(f"发生错误: {str(error)}", {
        "error_type": type(error).__name__,
        "context": context,
        "event_type": "error"
    })


def create_audit_log(action: str, resource_type: str, resource_id: str,
                    user_id: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
    """
    创建审计日志
    
    Args:
        action: 操作类型
        resource_type: 资源类型
        resource_id: 资源ID
        user_id: 用户ID
        details: 详细信息
    """
    logger = get_logger('audit')
    logger.info(f"审计日志: {action} {resource_type} {resource_id}", {
        "action": action,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "user_id": user_id,
        "details": details or {},
        "event_type": "audit"
    })


# 便捷函数
def debug(msg: str, **kwargs):
    """便捷调试日志函数"""
    get_logger().debug(msg, kwargs)


def info(msg: str, **kwargs):
    """便捷信息日志函数"""
    get_logger().info(msg, kwargs)


def warning(msg: str, **kwargs):
    """便捷警告日志函数"""
    get_logger().warning(msg, kwargs)


def error(msg: str, **kwargs):
    """便捷错误日志函数"""
    get_logger().error(msg, kwargs)


def critical(msg: str, **kwargs):
    """便捷严重错误日志函数"""
    get_logger().critical(msg, kwargs)


# 测试函数
if __name__ == "__main__":
    # 测试日志功能
    logger = get_logger('test')
    
    logger.debug("这是一条调试消息")
    logger.info("这是一条信息消息")
    logger.warning("这是一条警告消息")
    logger.error("这是一条错误消息")
    
    # 测试结构化日志
    logger.log_rss_fetch("新华网", True, 10)
    logger.log_analysis("news_123", True, True, 0.85)
    logger.log_performance("数据库查询", 0.123)
    
    print("日志测试完成")