import os
import json
import logging
from typing import Any, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class ConfigManager:
    """配置管理器 - 负责配置的加载、验证、更新和持久化"""
    
    def __init__(self, config_file='config.json', env_prefix='BSN_'):
        self.config_file = config_file
        self.env_prefix = env_prefix
        self.config = {}
        self._config_handlers = []
        self._load_config()
    
    def _load_config(self):
        """加载配置 - 优先级：环境变量 > 数据库 > 配置文件 > 默认配置"""
        # 1. 加载默认配置
        self.config = self._get_default_config()
        
        # 2. 加载文件配置
        if os.path.exists(self.config_file):
            file_config = self._load_file_config()
            self._deep_merge(self.config, file_config)
        
        # 3. 加载数据库配置
        db_config = self._load_database_config()
        self._deep_merge(self.config, db_config)
        
        # 4. 处理环境变量
        self._process_environment_variables()
        
        # 5. 验证配置
        self._validate_config()
    
    def _get_default_config(self):
        """获取默认配置"""
        return {
            "version": "1.0.0",
            "rss_sources": [],
            "llm_config": {
                "api_base": "https://api.openai.com/v1",
                "api_key": "",
                "model": "gpt-3.5-turbo",
                "max_tokens": 500,
                "temperature": 0.3,
                "timeout": 30,
                "max_retries": 3
            },
            "analysis_config": {
                "prompt_template": self._get_default_prompt(),
                "batch_size": 10,
                "confidence_threshold": 0.7,
                "black_swan_criteria": {
                    "min_surprise_score": 7,
                    "min_impact_score": 8
                }
            },
            "scheduler_config": {
                "rss_fetch_interval": 3600,
                "analysis_interval": 1800,
                "cleanup_interval": 86400,
                "retry_delay": 300
            },
            "app_config": {
                "debug": False,
                "host": "0.0.0.0",
                "port": 5000,
                "secret_key": "change-this-in-production",
                "database_url": "sqlite:///black_swan_news.db"
            },
            "ui_config": {
                "theme": "dark",
                "items_per_page": 20,
                "auto_refresh": True,
                "refresh_interval": 300
            }
        }
    
    def _get_default_prompt(self):
        """获取默认黑天鹅分析提示词"""
        return """请分析以下新闻内容，判断其是否属于黑天鹅事件。黑天鹅事件具有以下特征：
1. 意外性：事件出乎意料，超出正常预期范围
2. 重大影响：对经济、社会或市场产生重大冲击  
3. 事后可解释性：事后人们会为它的发生编造理由

请从以下维度分析：
- 意外程度（1-10分）：事件发生的不可预测程度
- 潜在影响范围（1-10分）：事件可能产生的广泛影响
- 是否是黑天鹅事件（是/否）：综合判断
- 简要分析理由：基于黑天鹅特征的分析

请以JSON格式返回分析结果：
{
    "surprise_score": 数字,
    "impact_score": 数字, 
    "is_black_swan": 布尔值,
    "analysis_reason": "分析理由文本",
    "confidence": 0.0到1.0的置信度
}

新闻内容：{news_content}"""
    
    def _load_file_config(self):
        """加载文件配置"""
        config = {}
        try:
            # 加载主配置文件
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            
            # 加载RSS源配置文件
            if os.path.exists('rss_sources.json'):
                with open('rss_sources.json', 'r', encoding='utf-8') as f:
                    config['rss_sources'] = json.load(f)
                    
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
        
        return config
    
    def _load_database_config(self):
        """从数据库加载配置"""
        # 这里应该实现从数据库加载配置的逻辑
        # 暂时返回空字典，实际实现需要数据库连接
        return {}
    
    def _process_environment_variables(self):
        """处理环境变量配置"""
        env_mappings = {
            'LITELLM_API_KEY': 'llm_config.api_key',
            'LITELLM_API_BASE': 'llm_config.api_base',
            'LITELLM_MODEL': 'llm_config.model',
            'DATABASE_URL': 'app_config.database_url',
            'APP_SECRET_KEY': 'app_config.secret_key',
            'DEBUG': 'app_config.debug',
            'HOST': 'app_config.host',
            'PORT': 'app_config.port',
            'MAX_TOKENS': 'llm_config.max_tokens',
            'TEMPERATURE': 'llm_config.temperature',
            'TIMEOUT': 'llm_config.timeout',
            'MAX_RETRIES': 'llm_config.max_retries'
        }
        
        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                # 类型转换
                if env_var in ['DEBUG']:
                    value = value.lower() in ('true', '1', 'yes')
                elif env_var in ['PORT', 'MAX_TOKENS', 'TIMEOUT', 'MAX_RETRIES']:
                    try:
                        value = int(value)
                    except ValueError:
                        continue
                elif env_var in ['TEMPERATURE']:
                    try:
                        value = float(value)
                    except ValueError:
                        continue
                
                self.set(config_path, value, persist=False)
    
    def _validate_config(self):
        """验证配置"""
        # 这里应该实现配置验证逻辑
        pass
    
    def _deep_merge(self, base: Dict, update: Dict) -> Dict:
        """深度合并字典"""
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value
        return base
    
    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值，支持点分隔符"""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value
    
    def set(self, key: str, value: Any, persist: bool = True) -> None:
        """设置配置值"""
        keys = key.split('.')
        config = self.config
        
        # 导航到父级配置
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        # 设置值
        config[keys[-1]] = value
        
        # 持久化到数据库
        if persist:
            self._save_to_database(key, value)
        
        # 通知配置变更
        self._notify_config_change(key, value)
    
    def add_rss_source(self, source_config: Dict) -> None:
        """添加RSS源"""
        if 'rss_sources' not in self.config:
            self.config['rss_sources'] = []
        
        # 生成唯一ID
        if 'id' not in source_config:
            source_config['id'] = f"source_{len(self.config['rss_sources']) + 1}"
        
        self.config['rss_sources'].append(source_config)
        self._save_rss_sources()
    
    def update_rss_source(self, source_id: str, updates: Dict) -> bool:
        """更新RSS源"""
        for source in self.config.get('rss_sources', []):
            if source.get('id') == source_id:
                source.update(updates)
                self._save_rss_sources()
                return True
        return False
    
    def _save_rss_sources(self) -> None:
        """保存RSS源配置到文件"""
        try:
            with open('rss_sources.json', 'w', encoding='utf-8') as f:
                json.dump(self.config.get('rss_sources', []), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存RSS源配置失败: {e}")
    
    def _save_to_database(self, key: str, value: Any) -> None:
        """保存配置到数据库"""
        # 这里应该实现保存到数据库的逻辑
        pass
    
    def _notify_config_change(self, key: str, value: Any) -> None:
        """通知配置变更"""
        # 这里应该实现配置变更通知逻辑
        pass