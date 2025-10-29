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
            analysis_result = classifier.analyze_news_sync(test_title, test_content)

            # # 1. 准备分析内容
            # analysis_content = self._prepare_analysis_content(news)
            #
            # # 2. 调用LLM API
            # llm_response = await self._call_llm_api(analysis_content)
            #
            # # 3. 解析响应
            # analysis_result = self._parse_llm_response(llm_response)
            
            # 4. 验证结果
            self._validate_analysis_result(analysis_result)
            
            # 5. 保存结果
            if self.db_manager:
                await self._save_analysis_result(news.id, analysis_result)
            
            self.logger.info(f"新闻分析完成: {news.title[:50]}... - 黑天鹅: {analysis_result.is_black_swan}")
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"新闻分析失败 {news.title[:50]}...: {str(e)}")

    
#     def _prepare_analysis_content(self, news):
#         """准备分析内容"""
#         # 从环境变量获取提示词模板，如果没有则使用默认值
#         prompt_template = os.getenv('ANALYSIS_PROMPT_TEMPLATE')
#         if not prompt_template:
#             prompt_template = """请分析以下新闻内容，判断其是否属于黑天鹅事件。黑天鹅事件具有以下特征：
# 1. 意外性：事件出乎意料，超出正常预期范围
# 2. 重大影响：对经济、社会或市场产生重大冲击
# 3. 事后可解释性：事后人们会为它的发生编造理由
#
# 请从以下维度分析：
# - 意外程度（1-10分）：事件发生的不可预测程度
# - 潜在影响范围（1-10分）：事件可能产生的广泛影响
# - 是否是黑天鹅事件（是/否）：综合判断
# - 简要分析理由：基于黑天鹅特征的分析
#
# 请以JSON格式返回分析结果：
# {
#     "surprise_score": 数字,
#     "impact_score": 数字,
#     "is_black_swan": 布尔值,
#     "analysis_reason": "分析理由文本",
#     "confidence": 0.0到1.0的置信度
# }
#
# 新闻内容：{news_content}"""
#
#         # 构建新闻内容
#         news_content = f"""
# 标题: {news.title}
#
# 摘要: {news.summary}
#
# 内容: {news.content[:2000]}  # 限制内容长度
# """
#
#         # 填充提示词模板
#         analysis_prompt = prompt_template.format(news_content=news_content)
#
#         return {
#             'messages': [
#                 {
#                     'role': 'system',
#                     'content': '你是一个专业的金融风险分析师，专门识别黑天鹅事件。请严格按照要求的JSON格式返回分析结果。'
#                 },
#                 {
#                     'role': 'user',
#                     'content': analysis_prompt
#                 }
#             ]
#         }
    
    # async def _call_llm_api(self, analysis_content):
    #     """调用LLM API"""
    #     try:
    #         # 使用LiteLLM进行调用
    #         response = await self.client.completion(
    #                 model=self.model,
    #                 messages=analysis_content['messages'],
    #                 max_tokens=1000,
    #                 temperature=0.1,
    #                 api_base=self.api_base,
    #                 api_key=self.api_key,
    #
    #         )
    #
    #         if not response or not hasattr(response, 'choices') or not response.choices:
    #             raise LLMAPIError("LLM API返回空响应")
    #         print("response:", response)
    #         return response.choices[0].message.content
    #
    #     except Exception as e:
    #         error_msg = str(e)
    #
    #         # 分类错误类型
    #         if "rate limit" in error_msg.lower():
    #             raise LLMRateLimitError(f"API速率限制: {error_msg}")
    #         elif "timeout" in error_msg.lower():
    #             raise LLMTimeoutError(f"API超时: {error_msg}")
    #         elif "authentication" in error_msg.lower() or "api key" in error_msg.lower():
    #             raise LLMAuthError(f"API认证失败: {error_msg}")
    #         else:
    #             raise LLMAPIError(f"API调用失败: {error_msg}")

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

# 重试装饰器
def retry_on_failure(max_retries=3, delay=2, exceptions=(Exception,)):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        await asyncio.sleep(delay * (2 ** attempt))  # 指数退避
                    else:
                        break
            raise last_exception
        return wrapper
    return decorator



if __name__ == "__main__":
    pass