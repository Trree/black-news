"""
辅助函数模块
提供各种通用辅助功能
"""

import re
import json
import hashlib
import datetime
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse
import html


def escape_html(text: str) -> str:
    """
    HTML转义函数
    
    Args:
        text: 需要转义的文本
        
    Returns:
        转义后的HTML安全文本
    """
    return html.escape(text)


def generate_hash(text: str, length: int = 8) -> str:
    """
    生成文本的哈希值
    
    Args:
        text: 输入文本
        length: 哈希长度
        
    Returns:
        哈希字符串
    """
    return hashlib.md5(text.encode()).hexdigest()[:length]


def format_timestamp(timestamp: Union[str, datetime.datetime]) -> str:
    """
    格式化时间戳为可读字符串
    
    Args:
        timestamp: 时间戳或日期时间对象
        
    Returns:
        格式化后的时间字符串
    """
    if isinstance(timestamp, str):
        try:
            timestamp = datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except ValueError:
            return timestamp
    
    if isinstance(timestamp, datetime.datetime):
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    return str(timestamp)


def truncate_text(text: str, max_length: int = 200, suffix: str = "...") -> str:
    """
    截断文本，保留指定长度
    
    Args:
        text: 输入文本
        max_length: 最大长度
        suffix: 截断后缀
        
    Returns:
        截断后的文本
    """
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def extract_domain(url: str) -> str:
    """
    从URL中提取域名
    
    Args:
        url: 完整的URL
        
    Returns:
        域名
    """
    try:
        parsed = urlparse(url)
        return parsed.netloc
    except Exception:
        return ""


def is_valid_url(url: str) -> bool:
    """
    验证URL格式是否有效
    
    Args:
        url: 待验证的URL
        
    Returns:
        是否有效
    """
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def clean_text(text: str) -> str:
    """
    清理文本，移除多余空格和换行
    
    Args:
        text: 输入文本
        
    Returns:
        清理后的文本
    """
    if not text:
        return ""
    
    # 移除多余空格和换行
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_json_safe(json_str: str, default: Any = None) -> Any:
    """
    安全解析JSON字符串
    
    Args:
        json_str: JSON字符串
        default: 解析失败时的默认值
        
    Returns:
        解析后的对象或默认值
    """
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return default


def to_json_safe(obj: Any, default: str = "{}") -> str:
    """
    安全转换为JSON字符串
    
    Args:
        obj: 要转换的对象
        default: 转换失败时的默认值
        
    Returns:
        JSON字符串
    """
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return default


def calculate_confidence_score(factors: Dict[str, float]) -> float:
    """
    计算综合置信度分数
    
    Args:
        factors: 各因素权重字典
        
    Returns:
        综合置信度 (0-1之间)
    """
    if not factors:
        return 0.0
    
    total_weight = sum(factors.values())
    if total_weight == 0:
        return 0.0
    
    # 加权平均
    weighted_sum = sum(score * weight for score, weight in factors.items())
    return min(1.0, max(0.0, weighted_sum / total_weight))


def format_number(number: Union[int, float]) -> str:
    """
    格式化数字显示
    
    Args:
        number: 输入数字
        
    Returns:
        格式化后的字符串
    """
    if isinstance(number, int):
        return f"{number:,}"
    
    if isinstance(number, float):
        if number >= 1000:
            return f"{number:,.1f}"
        elif number >= 1:
            return f"{number:.2f}"
        else:
            return f"{number:.3f}"
    
    return str(number)


def get_time_ago(timestamp: datetime.datetime) -> str:
    """
    获取相对时间描述
    
    Args:
        timestamp: 时间戳
        
    Returns:
        相对时间描述
    """
    now = datetime.datetime.now()
    diff = now - timestamp
    
    if diff.days > 365:
        years = diff.days // 365
        return f"{years}年前"
    elif diff.days > 30:
        months = diff.days // 30
        return f"{months}个月前"
    elif diff.days > 0:
        return f"{diff.days}天前"
    elif diff.seconds > 3600:
        hours = diff.seconds // 3600
        return f"{hours}小时前"
    elif diff.seconds > 60:
        minutes = diff.seconds // 60
        return f"{minutes}分钟前"
    else:
        return "刚刚"


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    将列表分割成指定大小的块
    
    Args:
        lst: 输入列表
        chunk_size: 每个块的大小
        
    Returns:
        分割后的块列表
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def safe_get(dictionary: Dict, keys: List[str], default: Any = None) -> Any:
    """
    安全获取嵌套字典的值
    
    Args:
        dictionary: 字典对象
        keys: 键路径列表
        default: 默认值
        
    Returns:
        获取的值或默认值
    """
    current = dictionary
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current


def remove_duplicates_preserve_order(lst: List[Any]) -> List[Any]:
    """
    移除列表重复项并保持顺序
    
    Args:
        lst: 输入列表
        
    Returns:
        去重后的列表
    """
    seen = set()
    result = []
    for item in lst:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def normalize_text(text: str) -> str:
    """
    标准化文本，用于比较
    
    Args:
        text: 输入文本
        
    Returns:
        标准化后的文本
    """
    if not text:
        return ""
    
    # 转换为小写，移除标点符号和多余空格
    text = text.lower()
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def calculate_similarity(text1: str, text2: str) -> float:
    """
    计算两个文本的相似度（基于词集）
    
    Args:
        text1: 文本1
        text2: 文本2
        
    Returns:
        相似度 (0-1之间)
    """
    if not text1 or not text2:
        return 0.0
    
    # 标准化文本
    text1 = normalize_text(text1)
    text2 = normalize_text(text2)
    
    if not text1 or not text2:
        return 0.0
    
    # 创建词集
    words1 = set(text1.split())
    words2 = set(text2.split())
    
    if not words1 or not words2:
        return 0.0
    
    # 计算Jaccard相似度
    intersection = len(words1.intersection(words2))
    union = len(words1.union(words2))
    
    return intersection / union if union > 0 else 0.0


def format_file_size(size_bytes: int) -> str:
    """
    格式化文件大小
    
    Args:
        size_bytes: 字节大小
        
    Returns:
        格式化后的文件大小字符串
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.2f} {size_names[i]}"


def validate_email(email: str) -> bool:
    """
    验证邮箱格式
    
    Args:
        email: 邮箱地址
        
    Returns:
        是否有效
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def get_current_time() -> str:
    """
    获取当前时间的ISO格式字符串
    
    Returns:
        ISO格式时间字符串
    """
    return datetime.datetime.now().isoformat()


def deep_update_dict(original: Dict, update: Dict) -> Dict:
    """
    深度更新字典
    
    Args:
        original: 原始字典
        update: 更新字典
        
    Returns:
        更新后的字典
    """
    result = original.copy()
    for key, value in update.items():
        if isinstance(value, dict) and key in result and isinstance(result[key], dict):
            result[key] = deep_update_dict(result[key], value)
        else:
            result[key] = value
    return result