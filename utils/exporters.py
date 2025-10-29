"""
导出器模块
提供数据导出功能
"""

import csv
import json
import io
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd


def export_to_csv(data: List[Dict[str, Any]], 
                 fields: Optional[List[str]] = None,
                 filename: Optional[str] = None) -> tuple:
    """
    导出数据到CSV格式
    
    Args:
        data: 要导出的数据列表
        fields: 指定导出的字段，如果为None则导出所有字段
        filename: 文件名（可选）
        
    Returns:
        (CSV内容, 文件名)
    """
    if not data:
        return "", filename or "empty.csv"
    
    # 如果没有指定字段，使用所有字段
    if not fields:
        fields = list(data[0].keys())
    
    # 创建内存中的CSV文件
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields)
    
    # 写入表头
    writer.writeheader()
    
    # 写入数据
    for row in data:
        # 只保留指定的字段
        filtered_row = {field: row.get(field, '') for field in fields}
        writer.writerow(filtered_row)
    
    csv_content = output.getvalue()
    output.close()
    
    # 生成文件名
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"black_swan_news_{timestamp}.csv"
    
    return csv_content, filename


def export_to_json(data: List[Dict[str, Any]], 
                  filename: Optional[str] = None,
                  pretty: bool = True) -> tuple:
    """
    导出数据到JSON格式
    
    Args:
        data: 要导出的数据列表
        filename: 文件名（可选）
        pretty: 是否美化输出
        
    Returns:
        (JSON内容, 文件名)
    """
    if not data:
        return "[]", filename or "empty.json"
    
    # 转换为JSON字符串
    if pretty:
        json_content = json.dumps(data, ensure_ascii=False, indent=2, default=str)
    else:
        json_content = json.dumps(data, ensure_ascii=False, default=str)
    
    # 生成文件名
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"black_swan_news_{timestamp}.json"
    
    return json_content, filename


def export_news_data(news_list: List[Dict[str, Any]], 
                    format_type: str = 'csv',
                    include_analysis: bool = True) -> tuple:
    """
    导出新闻数据
    
    Args:
        news_list: 新闻数据列表
        format_type: 导出格式 ('csv' 或 'json')
        include_analysis: 是否包含分析结果
        
    Returns:
        (导出内容, 文件名)
    """
    if not news_list:
        return "", f"empty.{format_type}"
    
    # 准备导出数据
    export_data = []
    
    for news in news_list:
        # 基础新闻信息
        news_data = {
            'id': news.get('id'),
            'title': news.get('title', ''),
            'summary': news.get('summary', ''),
            'url': news.get('url', ''),
            'source_name': news.get('source_name', ''),
            'published_at': news.get('published_at', ''),
            'image_url': news.get('image_url', ''),
            'created_at': news.get('created_at', '')
        }
        
        # 如果包含分析结果
        if include_analysis and 'analysis_result' in news:
            analysis = news['analysis_result']
            if analysis:
                news_data.update({
                    'is_black_swan': analysis.get('is_black_swan', False),
                    'confidence': analysis.get('confidence', 0),
                    'risk_level': analysis.get('risk_level', ''),
                    'reasoning': analysis.get('reasoning', ''),
                    'verified': analysis.get('verified', False),
                    'analysis_created_at': analysis.get('created_at', '')
                })
        
        export_data.append(news_data)
    
    # 根据格式导出
    if format_type.lower() == 'json':
        return export_to_json(export_data)
    else:
        # 默认导出CSV
        return export_to_csv(export_data)


def export_analysis_report(news_list: List[Dict[str, Any]], 
                          format_type: str = 'csv') -> tuple:
    """
    导出分析报告
    
    Args:
        news_list: 包含分析结果的新闻列表
        format_type: 导出格式 ('csv' 或 'json')
        
    Returns:
        (导出内容, 文件名)
    """
    if not news_list:
        return "", f"analysis_report_empty.{format_type}"
    
    # 过滤出有分析结果的新闻
    analyzed_news = [
        news for news in news_list 
        if news.get('analysis_result')
    ]
    
    if not analyzed_news:
        return "", f"no_analysis_data.{format_type}"
    
    # 准备分析报告数据
    report_data = []
    
    for news in analyzed_news:
        analysis = news['analysis_result']
        
        report_item = {
            'news_id': news.get('id'),
            'news_title': news.get('title', ''),
            'source': news.get('source_name', ''),
            'published_at': news.get('published_at', ''),
            'is_black_swan': analysis.get('is_black_swan', False),
            'confidence': analysis.get('confidence', 0),
            'risk_level': analysis.get('risk_level', ''),
            'verified': analysis.get('verified', False),
            'analysis_timestamp': analysis.get('created_at', ''),
            'reasoning_summary': _summarize_reasoning(analysis.get('reasoning', ''))
        }
        
        report_data.append(report_item)
    
    # 根据格式导出
    if format_type.lower() == 'json':
        return export_to_json(report_data, filename=f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    else:
        return export_to_csv(report_data, filename=f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")


def _summarize_reasoning(reasoning: str, max_length: int = 200) -> str:
    """
    总结推理过程
    
    Args:
        reasoning: 完整的推理过程
        max_length: 最大长度
        
    Returns:
        总结后的推理过程
    """
    if not reasoning:
        return ""
    
    if len(reasoning) <= max_length:
        return reasoning
    
    # 截断并添加省略号
    return reasoning[:max_length-3] + "..."


def export_statistics(stats_data: Dict[str, Any], 
                     format_type: str = 'json') -> tuple:
    """
    导出统计数据
    
    Args:
        stats_data: 统计数据字典
        format_type: 导出格式 ('csv' 或 'json')
        
    Returns:
        (导出内容, 文件名)
    """
    if not stats_data:
        return "", f"statistics_empty.{format_type}"
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if format_type.lower() == 'csv':
        # 将统计数据转换为适合CSV的格式
        csv_data = []
        
        # 基础统计
        if 'general' in stats_data:
            for key, value in stats_data['general'].items():
                csv_data.append({
                    'category': 'general',
                    'metric': key,
                    'value': value
                })
        
        # 来源统计
        if 'sources' in stats_data:
            for source_stat in stats_data['sources']:
                csv_data.append({
                    'category': 'sources',
                    'metric': source_stat.get('name', ''),
                    'value': source_stat.get('count', 0),
                    'success_rate': source_stat.get('success_rate', 0)
                })
        
        # 时间统计
        if 'time_series' in stats_data:
            for time_stat in stats_data['time_series']:
                csv_data.append({
                    'category': 'time_series',
                    'date': time_stat.get('date', ''),
                    'news_count': time_stat.get('news_count', 0),
                    'black_swan_count': time_stat.get('black_swan_count', 0)
                })
        
        return export_to_csv(csv_data, filename=f"statistics_{timestamp}.csv")
    
    else:
        # JSON格式直接导出
        return export_to_json(stats_data, filename=f"statistics_{timestamp}.json", pretty=True)


def export_sources_data(sources_list: List[Dict[str, Any]], 
                       format_type: str = 'csv') -> tuple:
    """
    导出数据源信息
    
    Args:
        sources_list: 数据源列表
        format_type: 导出格式 ('csv' 或 'json')
        
    Returns:
        (导出内容, 文件名)
    """
    if not sources_list:
        return "", f"sources_empty.{format_type}"
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 准备导出数据
    export_data = []
    
    for source in sources_list:
        source_data = {
            'id': source.get('id'),
            'name': source.get('name', ''),
            'url': source.get('url', ''),
            'category': source.get('category', ''),
            'is_active': source.get('is_active', False),
            'last_fetch': source.get('last_fetch', ''),
            'success_count': source.get('success_count', 0),
            'failure_count': source.get('failure_count', 0),
            'created_at': source.get('created_at', ''),
            'updated_at': source.get('updated_at', '')
        }
        
        # 计算成功率
        total_attempts = source_data['success_count'] + source_data['failure_count']
        source_data['success_rate'] = round(
            (source_data['success_count'] / total_attempts * 100) if total_attempts > 0 else 0, 2
        )
        
        export_data.append(source_data)
    
    if format_type.lower() == 'json':
        return export_to_json(export_data, filename=f"sources_{timestamp}.json")
    else:
        return export_to_csv(export_data, filename=f"sources_{timestamp}.csv")


def create_excel_report(news_data: List[Dict[str, Any]], 
                       sources_data: List[Dict[str, Any]],
                       stats_data: Dict[str, Any]) -> tuple:
    """
    创建Excel综合报告（需要pandas）
    
    Args:
        news_data: 新闻数据
        sources_data: 数据源信息
        stats_data: 统计数据
        
    Returns:
        (Excel文件内容, 文件名)
    """
    try:
        # 创建Excel写入器
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # 新闻数据表
            if news_data:
                news_df = pd.DataFrame(news_data)
                # 处理嵌套的分析结果
                if 'analysis_result' in news_df.columns:
                    # 展开分析结果
                    analysis_expanded = news_df['analysis_result'].apply(
                        lambda x: x if isinstance(x, dict) else {}
                    )
                    analysis_df = pd.json_normalize(analysis_expanded)
                    # 合并新闻数据和分析结果
                    news_df = pd.concat([news_df.drop('analysis_result', axis=1), analysis_df], axis=1)
                
                news_df.to_excel(writer, sheet_name='News', index=False)
            
            # 数据源表
            if sources_data:
                sources_df = pd.DataFrame(sources_data)
                sources_df.to_excel(writer, sheet_name='Sources', index=False)
            
            # 统计表
            if stats_data:
                # 转换统计数据为DataFrame
                stats_rows = []
                for category, metrics in stats_data.items():
                    if isinstance(metrics, dict):
                        for metric, value in metrics.items():
                            stats_rows.append({'Category': category, 'Metric': metric, 'Value': value})
                    elif isinstance(metrics, list):
                        for item in metrics:
                            if isinstance(item, dict):
                                item['Category'] = category
                                stats_rows.append(item)
                
                if stats_rows:
                    stats_df = pd.DataFrame(stats_rows)
                    stats_df.to_excel(writer, sheet_name='Statistics', index=False)
        
        excel_content = output.getvalue()
        output.close()
        
        filename = f"black_swan_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        return excel_content, filename
        
    except ImportError:
        raise ImportError("pandas和openpyxl库是创建Excel报告所必需的")
    except Exception as e:
        raise Exception(f"创建Excel报告失败: {str(e)}")


def export_filtered_data(data: List[Dict[str, Any]], 
                        filters: Dict[str, Any],
                        format_type: str = 'csv') -> tuple:
    """
    导出筛选后的数据
    
    Args:
        data: 原始数据
        filters: 筛选条件
        format_type: 导出格式
        
    Returns:
        (导出内容, 文件名)
    """
    if not data:
        return "", f"filtered_empty.{format_type}"
    
    # 应用筛选条件
    filtered_data = _apply_filters(data, filters)
    
    if not filtered_data:
        return "", f"no_matching_data.{format_type}"
    
    # 根据格式导出
    if format_type.lower() == 'json':
        return export_to_json(filtered_data)
    else:
        return export_to_csv(filtered_data)


def _apply_filters(data: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    应用筛选条件到数据
    
    Args:
        data: 原始数据
        filters: 筛选条件
        
    Returns:
        筛选后的数据
    """
    filtered_data = data
    
    # 时间范围筛选
    if 'start_date' in filters and filters['start_date']:
        start_date = datetime.fromisoformat(filters['start_date'].replace('Z', '+00:00'))
        filtered_data = [
            item for item in filtered_data 
            if datetime.fromisoformat(item.get('published_at', '').replace('Z', '+00:00')) >= start_date
        ]
    
    if 'end_date' in filters and filters['end_date']:
        end_date = datetime.fromisoformat(filters['end_date'].replace('Z', '+00:00'))
        filtered_data = [
            item for item in filtered_data 
            if datetime.fromisoformat(item.get('published_at', '').replace('Z', '+00:00')) <= end_date
        ]
    
    # 事件类型筛选
    if 'event_type' in filters and filters['event_type']:
        if filters['event_type'] == 'black_swan':
            filtered_data = [
                item for item in filtered_data 
                if item.get('analysis_result', {}).get('is_black_swan', False)
            ]
        elif filters['event_type'] == 'normal':
            filtered_data = [
                item for item in filtered_data 
                if not item.get('analysis_result', {}).get('is_black_swan', True)
            ]
    
    # 风险等级筛选
    if 'risk_level' in filters and filters['risk_level']:
        filtered_data = [
            item for item in filtered_data 
            if item.get('analysis_result', {}).get('risk_level', '') == filters['risk_level']
        ]
    
    # 数据源筛选
    if 'source' in filters and filters['source']:
        filtered_data = [
            item for item in filtered_data 
            if item.get('source_name', '') == filters['source']
        ]
    
    # 置信度筛选
    if 'min_confidence' in filters and filters['min_confidence']:
        min_confidence = float(filters['min_confidence'])
        filtered_data = [
            item for item in filtered_data 
            if item.get('analysis_result', {}).get('confidence', 0) >= min_confidence
        ]
    
    return filtered_data


def get_export_formats() -> Dict[str, str]:
    """
    获取支持的导出格式
    
    Returns:
        格式名称和描述的字典
    """
    return {
        'csv': 'CSV格式 - 适合电子表格软件',
        'json': 'JSON格式 - 适合程序处理',
        'excel': 'Excel格式 - 综合报告（需要pandas）'
    }


def validate_export_format(format_type: str) -> bool:
    """
    验证导出格式是否支持
    
    Args:
        format_type: 导出格式
        
    Returns:
        是否支持
    """
    supported_formats = ['csv', 'json', 'excel']
    return format_type.lower() in supported_formats