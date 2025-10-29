import asyncio
import json
import logging
import os
import re
from datetime import datetime
from typing import List, Any

from core.gpt_classifier import GPTBlackSwanClassifier

logger = logging.getLogger(__name__)

class LLMError(Exception):
    """LLM相关错误基类"""
    pass

class LLMInitializationError(LLMError):
    """LLM初始化错误"""
    pass

class LLMAPIError(LLMError):
    """LLM API调用错误"""
    pass

class LLMRateLimitError(LLMAPIError):
    """API速率限制错误"""
    pass

class LLMTimeoutError(LLMAPIError):
    """API超时错误"""
    pass

class LLMAuthError(LLMAPIError):
    """API认证错误"""
    pass

class LLMResponseParseError(LLMError):
    """响应解析错误"""
    pass

class LLMValidationError(LLMError):
    """结果验证错误"""
    pass

class LLMSaveError(LLMError):
    """保存错误"""
    pass

class AnalysisResult:
    """分析结果数据模型"""
    
    def __init__(self):
        self.id = None
        self.news_id = None
        self.is_black_swan = False
        self.surprise_score = 0
        self.impact_score = 0
        self.analysis_reason = ""
        self.confidence = 0.0
        self.analyzed_at = None
        self.black_swan_score = 0.0
    
    def to_dict(self):
        """转换为字典"""
        return {
            'id': self.id,
            'news_id': self.news_id,
            'is_black_swan': self.is_black_swan,
            'surprise_score': self.surprise_score,
            'impact_score': self.impact_score,
            'analysis_reason': self.analysis_reason,
            'confidence': self.confidence,
            'analyzed_at': self.analyzed_at.isoformat() if self.analyzed_at else None,
            'black_swan_score': self.black_swan_score
        }

class LLMAnalyzer:
    """LiteLLM分析器 - 负责调用LLM API进行黑天鹅事件分析"""
    
    def __init__(self, config_manager=None, db_manager=None):
        self.config_manager = config_manager
        self.db_manager = db_manager
        self.client = None
        self.logger = logging.getLogger(__name__)

    async def analyze_news_batch(self, news_list: List[Any], batch_size: int = 10):
        """批量分析新闻"""
        # 从环境变量获取批处理大小配置，如果没有则使用默认值
        batch_size = int(os.getenv('ANALYSIS_BATCH_SIZE', str(batch_size)))
        
        results = []
        for i in range(0, len(news_list), batch_size):
            batch = news_list[i:i + batch_size]
            batch_results = await self._analyze_batch(batch)
            results.extend(batch_results)
            
            # 批量间延迟，避免API限制
            await asyncio.sleep(1)
        
        return results
    
    async def _analyze_batch(self, news_batch: List[Any]):
        """分析一批新闻"""
        tasks = []
        for news in news_batch:
            task = asyncio.create_task(self.analyze_single_news(news))
            tasks.append(task)
        
        return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def analyze_single_news(self, news):
        """分析单条新闻"""
        try:
            self.logger.info(f"开始分析新闻: {news.title[:50]}...")
            classifier = GPTBlackSwanClassifier()
            test_title = news.title
            test_content = news.content
            llm_response = classifier.analyze_news_sync(test_title, test_content)
            analysis_result = self._parse_llm_response(llm_response)
            self._validate_analysis_result(analysis_result)

            if self.db_manager:
                await self._save_analysis_result(news.id, analysis_result)
            
            self.logger.info(f"新闻分析完成: {news.title[:50]}... - 黑天鹅: {analysis_result.is_black_swan}")
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"新闻分析失败 {news.title[:50]}...: {str(e)}")

    def _parse_llm_response(self, llm_response):
        """解析LLM响应"""
        try:
            # 尝试从响应中提取JSON
            json_match = re.search(r'\{.*\}', llm_response, re.DOTALL)
            if not json_match:
                raise LLMResponseParseError("未找到JSON格式的响应")
            
            json_str = json_match.group()
            result_data = json.loads(json_str)
            
            # 创建分析结果对象
            analysis_result = AnalysisResult()
            analysis_result.is_black_swan = result_data.get('is_black_swan', False)
            analysis_result.surprise_score = result_data.get('surprise_score', 0)
            analysis_result.impact_score = result_data.get('impact_score', 0)
            analysis_result.analysis_reason = result_data.get('analysis_reason', '')
            analysis_result.confidence = result_data.get('confidence', 0.0)
            
            return analysis_result
            
        except json.JSONDecodeError as e:
            raise LLMResponseParseError(f"JSON解析失败: {str(e)}")
        except Exception as e:
            raise LLMResponseParseError(f"响应解析失败: {str(e)}")
    
    def _validate_analysis_result(self, analysis_result):
        """验证分析结果"""
        errors = []
        
        # 验证分数范围
        if not (1 <= analysis_result.surprise_score <= 10):
            errors.append(f"意外程度评分超出范围: {analysis_result.surprise_score}")
        
        if not (1 <= analysis_result.impact_score <= 10):
            errors.append(f"影响程度评分超出范围: {analysis_result.impact_score}")
        
        # 验证置信度
        if not (0 <= analysis_result.confidence <= 1):
            errors.append(f"置信度超出范围: {analysis_result.confidence}")
        
        # 验证分析理由
        if not analysis_result.analysis_reason or len(analysis_result.analysis_reason.strip()) < 10:
            errors.append("分析理由过短或为空")
        
        if errors:
            raise LLMValidationError("; ".join(errors))
    
    async def _save_analysis_result(self, news_id, analysis_result):
        """保存分析结果到数据库"""
        try:
            if not self.db_manager:
                return
                
            await self.db_manager.execute(
                """
                INSERT INTO analysis_results 
                (news_id, is_black_swan, surprise_score, impact_score, analysis_reason, confidence, analyzed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    news_id,
                    analysis_result.is_black_swan,
                    analysis_result.surprise_score,
                    analysis_result.impact_score,
                    analysis_result.analysis_reason,
                    analysis_result.confidence,
                    datetime.utcnow()
                )
            )
        except Exception as e:
            raise LLMSaveError(f"保存分析结果失败: {str(e)}")

    def analyze_news_sync(self, content):
        """同步分析新闻（用于非异步环境）"""
        import asyncio
        return asyncio.run(self._call_llm_api(content))

if __name__ == "__main__":
    pass
