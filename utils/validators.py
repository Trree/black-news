"""
验证器模块
提供各种数据验证功能
"""

import re
import json
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime


def validate_rss_url(url: str) -> Tuple[bool, str]:
    """
    验证RSS URL格式
    
    Args:
        url: RSS URL
        
    Returns:
        (是否有效, 错误消息)
    """
    if not url:
        return False, "URL不能为空"
    
    # 基本URL格式验证
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return False, "URL格式不正确"
        
        if parsed.scheme not in ['http', 'https']:
            return False, "只支持HTTP和HTTPS协议"
    except Exception:
        return False, "URL解析失败"
    
    # 常见RSS文件扩展名检查
    rss_patterns = [
        r'\.rss$',
        r'\.xml$',
        r'feed',
        r'rss',
        r'atom'
    ]
    
    path_lower = parsed.path.lower()
    if not any(re.search(pattern, path_lower) for pattern in rss_patterns):
        return False, "URL可能不是有效的RSS源"
    
    return True, "URL格式正确"


def validate_news_data(news_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    验证新闻数据
    
    Args:
        news_data: 新闻数据字典
        
    Returns:
        (是否有效, 错误消息)
    """
    required_fields = ['title', 'url', 'source_name']
    
    for field in required_fields:
        if field not in news_data:
            return False, f"缺少必要字段: {field}"
        
        if not news_data[field]:
            return False, f"字段不能为空: {field}"
    
    # 验证URL
    if not is_valid_url(news_data['url']):
        return False, "新闻URL格式不正确"
    
    # 验证标题长度
    if len(news_data['title']) > 500:
        return False, "标题过长"
    
    # 验证摘要长度（如果存在）
    if 'summary' in news_data and news_data['summary']:
        if len(news_data['summary']) > 2000:
            return False, "摘要过长"
    
    # 验证发布时间（如果存在）
    if 'published_at' in news_data and news_data['published_at']:
        try:
            datetime.fromisoformat(news_data['published_at'].replace('Z', '+00:00'))
        except ValueError:
            return False, "发布时间格式不正确"
    
    return True, "新闻数据验证通过"


def validate_analysis_result(analysis_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    验证分析结果数据
    
    Args:
        analysis_data: 分析结果数据
        
    Returns:
        (是否有效, 错误消息)
    """
    required_fields = ['is_black_swan', 'confidence']
    
    for field in required_fields:
        if field not in analysis_data:
            return False, f"缺少必要字段: {field}"
    
    # 验证置信度
    confidence = analysis_data['confidence']
    if not isinstance(confidence, (int, float)):
        return False, "置信度必须是数字"
    
    if confidence < 0 or confidence > 1:
        return False, "置信度必须在0-1之间"
    
    # 验证黑天鹅标志
    if not isinstance(analysis_data['is_black_swan'], bool):
        return False, "is_black_swan必须是布尔值"
    
    # 验证推理过程（如果存在）
    if 'reasoning' in analysis_data and analysis_data['reasoning']:
        if not isinstance(analysis_data['reasoning'], str):
            return False, "推理过程必须是字符串"
        
        if len(analysis_data['reasoning']) > 5000:
            return False, "推理过程过长"
    
    # 验证风险等级（如果存在）
    if 'risk_level' in analysis_data and analysis_data['risk_level']:
        valid_risk_levels = ['low', 'medium', 'high', 'critical']
        if analysis_data['risk_level'] not in valid_risk_levels:
            return False, f"风险等级必须是: {', '.join(valid_risk_levels)}"
    
    return True, "分析结果验证通过"


def validate_config_data(config_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    验证配置数据
    
    Args:
        config_data: 配置数据
        
    Returns:
        (是否有效, 错误消息)
    """
    # 验证基本配置结构
    if not isinstance(config_data, dict):
        return False, "配置数据必须是字典"
    
    # 验证RSS配置
    if 'rss' in config_data:
        rss_config = config_data['rss']
        if not isinstance(rss_config, dict):
            return False, "RSS配置必须是字典"
        
        # 验证更新间隔
        if 'update_interval' in rss_config:
            interval = rss_config['update_interval']
            if not isinstance(interval, int) or interval < 300:
                return False, "更新间隔必须是不小于300秒的整数"
    
    # 验证LLM配置
    if 'llm' in config_data:
        llm_config = config_data['llm']
        if not isinstance(llm_config, dict):
            return False, "LLM配置必须是字典"
        
        # 验证模型名称
        if 'model_name' in llm_config:
            model_name = llm_config['model_name']
            if not model_name or not isinstance(model_name, str):
                return False, "模型名称不能为空"
        
        # 验证API密钥（如果存在）
        if 'api_key' in llm_config and llm_config['api_key']:
            if not isinstance(llm_config['api_key'], str):
                return False, "API密钥必须是字符串"
    
    # 验证数据库配置
    if 'database' in config_data:
        db_config = config_data['database']
        if not isinstance(db_config, dict):
            return False, "数据库配置必须是字典"
        
        # 验证数据库路径
        if 'path' in db_config:
            db_path = db_config['path']
            if not db_path or not isinstance(db_path, str):
                return False, "数据库路径不能为空"
    
    return True, "配置数据验证通过"


def validate_source_data(source_data: Dict[str, Any]) -> Tuple[bool, str]:
    """
    验证数据源数据
    
    Args:
        source_data: 数据源数据
        
    Returns:
        (是否有效, 错误消息)
    """
    required_fields = ['name', 'url']
    
    for field in required_fields:
        if field not in source_data:
            return False, f"缺少必要字段: {field}"
        
        if not source_data[field]:
            return False, f"字段不能为空: {field}"
    
    # 验证名称长度
    if len(source_data['name']) > 100:
        return False, "数据源名称过长"
    
    # 验证URL
    is_valid, message = validate_rss_url(source_data['url'])
    if not is_valid:
        return False, f"数据源URL无效: {message}"
    
    # 验证分类（如果存在）
    if 'category' in source_data and source_data['category']:
        valid_categories = ['news', 'finance', 'technology', 'politics', 'sports', 'entertainment', 'other']
        if source_data['category'] not in valid_categories:
            return False, f"分类必须是: {', '.join(valid_categories)}"
    
    # 验证是否激活标志（如果存在）
    if 'is_active' in source_data:
        if not isinstance(source_data['is_active'], bool):
            return False, "is_active必须是布尔值"
    
    return True, "数据源验证通过"


def validate_search_params(params: Dict[str, Any]) -> Tuple[bool, str]:
    """
    验证搜索参数
    
    Args:
        params: 搜索参数
        
    Returns:
        (是否有效, 错误消息)
    """
    # 验证页码
    if 'page' in params:
        try:
            page = int(params['page'])
            if page < 1:
                return False, "页码必须大于0"
        except (ValueError, TypeError):
            return False, "页码必须是整数"
    
    # 验证每页数量
    if 'per_page' in params:
        try:
            per_page = int(params['per_page'])
            if per_page < 1 or per_page > 100:
                return False, "每页数量必须在1-100之间"
        except (ValueError, TypeError):
            return False, "每页数量必须是整数"
    
    # 验证搜索关键词长度
    if 'search' in params and params['search']:
        if len(params['search']) > 200:
            return False, "搜索关键词过长"
    
    # 验证排序字段
    if 'sort_by' in params and params['sort_by']:
        valid_sort_fields = ['date_desc', 'date_asc', 'impact_desc', 'impact_asc', 'confidence_desc']
        if params['sort_by'] not in valid_sort_fields:
            return False, f"排序字段必须是: {', '.join(valid_sort_fields)}"
    
    # 验证事件类型筛选
    if 'event_type' in params and params['event_type']:
        valid_event_types = ['black_swan', 'normal']
        if params['event_type'] not in valid_event_types:
            return False, f"事件类型必须是: {', '.join(valid_event_types)}"
    
    # 验证风险等级筛选
    if 'risk_level' in params and params['risk_level']:
        valid_risk_levels = ['low', 'medium', 'high', 'confirmed']
        if params['risk_level'] not in valid_risk_levels:
            return False, f"风险等级必须是: {', '.join(valid_risk_levels)}"
    
    # 验证时间范围筛选
    if 'time_range' in params and params['time_range']:
        valid_time_ranges = ['today', 'week', 'month', 'year']
        if params['time_range'] not in valid_time_ranges:
            return False, f"时间范围必须是: {', '.join(valid_time_ranges)}"
    
    return True, "搜索参数验证通过"


def validate_export_params(params: Dict[str, Any]) -> Tuple[bool, str]:
    """
    验证导出参数
    
    Args:
        params: 导出参数
        
    Returns:
        (是否有效, 错误消息)
    """
    # 验证导出格式
    if 'format' in params and params['format']:
        valid_formats = ['csv', 'json']
        if params['format'] not in valid_formats:
            return False, f"导出格式必须是: {', '.join(valid_formats)}"
    
    # 验证时间范围
    if 'start_date' in params and params['start_date']:
        try:
            datetime.fromisoformat(params['start_date'].replace('Z', '+00:00'))
        except ValueError:
            return False, "开始日期格式不正确"
    
    if 'end_date' in params and params['end_date']:
        try:
            datetime.fromisoformat(params['end_date'].replace('Z', '+00:00'))
        except ValueError:
            return False, "结束日期格式不正确"
    
    # 验证字段选择
    if 'fields' in params and params['fields']:
        if not isinstance(params['fields'], list):
            return False, "字段必须是列表"
        
        valid_fields = ['title', 'summary', 'url', 'source_name', 'published_at', 
                       'is_black_swan', 'confidence', 'risk_level', 'reasoning']
        
        for field in params['fields']:
            if field not in valid_fields:
                return False, f"无效字段: {field}"
    
    return True, "导出参数验证通过"


def validate_user_input(text: str, max_length: int = 1000) -> Tuple[bool, str]:
    """
    验证用户输入文本
    
    Args:
        text: 输入文本
        max_length: 最大长度
        
    Returns:
        (是否有效, 错误消息)
    """
    if not text:
        return True, "输入为空"
    
    if len(text) > max_length:
        return False, f"输入文本过长，最大允许{max_length}字符"
    
    # 检查危险字符（基本的XSS防护）
    dangerous_patterns = [
        r'<script.*?>.*?</script>',
        r'javascript:',
        r'on\w+\s*=',
        r'<iframe.*?>.*?</iframe>'
    ]
    
    for pattern in dangerous_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return False, "输入包含不安全内容"
    
    return True, "输入验证通过"


def validate_email(email: str) -> Tuple[bool, str]:
    """
    验证邮箱格式
    
    Args:
        email: 邮箱地址
        
    Returns:
        (是否有效, 错误消息)
    """
    if not email:
        return False, "邮箱不能为空"
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if not re.match(pattern, email):
        return False, "邮箱格式不正确"
    
    return True, "邮箱格式正确"


def validate_password(password: str) -> Tuple[bool, str]:
    """
    验证密码强度
    
    Args:
        password: 密码
        
    Returns:
        (是否有效, 错误消息)
    """
    if not password:
        return False, "密码不能为空"
    
    if len(password) < 8:
        return False, "密码长度至少8位"
    
    # 检查包含数字
    if not re.search(r'\d', password):
        return False, "密码必须包含数字"
    
    # 检查包含字母
    if not re.search(r'[a-zA-Z]', password):
        return False, "密码必须包含字母"
    
    # 检查包含特殊字符
    if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
        return False, "密码必须包含特殊字符"
    
    return True, "密码强度足够"


def is_valid_url(url: str) -> bool:
    """
    检查URL格式是否有效
    
    Args:
        url: URL字符串
        
    Returns:
        是否有效
    """
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def validate_json_schema(data: Any, schema: Dict[str, Any]) -> Tuple[bool, str]:
    """
    简单的JSON模式验证
    
    Args:
        data: 要验证的数据
        schema: 验证模式
        
    Returns:
        (是否有效, 错误消息)
    """
    if not isinstance(schema, dict):
        return False, "验证模式必须是字典"
    
    # 检查必需字段
    required_fields = schema.get('required', [])
    for field in required_fields:
        if field not in data:
            return False, f"缺少必需字段: {field}"
    
    # 检查字段类型
    properties = schema.get('properties', {})
    for field, field_schema in properties.items():
        if field in data:
            field_type = field_schema.get('type')
            if field_type and not isinstance(data[field], _get_python_type(field_type)):
                return False, f"字段 '{field}' 类型不正确，期望 {field_type}"
            
            # 检查枚举值
            enum_values = field_schema.get('enum')
            if enum_values and data[field] not in enum_values:
                return False, f"字段 '{field}' 的值不在允许范围内"
            
            # 检查字符串长度
            if field_type == 'string':
                min_length = field_schema.get('minLength', 0)
                max_length = field_schema.get('maxLength', float('inf'))
                
                if len(str(data[field])) < min_length:
                    return False, f"字段 '{field}' 长度过短，最小 {min_length} 字符"
                
                if len(str(data[field])) > max_length:
                    return False, f"字段 '{field}' 长度过长，最大 {max_length} 字符"
    
    return True, "JSON模式验证通过"


def _get_python_type(type_name: str) -> type:
    """
    将JSON类型名称转换为Python类型
    
    Args:
        type_name: JSON类型名称
        
    Returns:
        Python类型
    """
    type_mapping = {
        'string': str,
        'number': (int, float),
        'integer': int,
        'boolean': bool,
        'array': list,
        'object': dict,
        'null': type(None)
    }
    
    return type_mapping.get(type_name, object)


# 预定义的验证模式
NEWS_SCHEMA = {
    'type': 'object',
    'required': ['title', 'url', 'source_name'],
    'properties': {
        'title': {'type': 'string', 'minLength': 1, 'maxLength': 500},
        'url': {'type': 'string'},
        'source_name': {'type': 'string', 'minLength': 1, 'maxLength': 100},
        'summary': {'type': 'string', 'maxLength': 2000},
        'published_at': {'type': 'string'},
        'image_url': {'type': 'string'}
    }
}

ANALYSIS_SCHEMA = {
    'type': 'object',
    'required': ['is_black_swan', 'confidence'],
    'properties': {
        'is_black_swan': {'type': 'boolean'},
        'confidence': {'type': 'number', 'minimum': 0, 'maximum': 1},
        'reasoning': {'type': 'string', 'maxLength': 5000},
        'risk_level': {'type': 'string', 'enum': ['low', 'medium', 'high', 'critical']},
        'verified': {'type': 'boolean'}
    }
}

SOURCE_SCHEMA = {
    'type': 'object',
    'required': ['name', 'url'],
    'properties': {
        'name': {'type': 'string', 'minLength': 1, 'maxLength': 100},
        'url': {'type': 'string'},
        'category': {'type': 'string', 'enum': ['news', 'finance', 'technology', 'politics', 'sports', 'entertainment', 'other']},
        'is_active': {'type': 'boolean'}
    }
}