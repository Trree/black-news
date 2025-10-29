"""
黑天鹅新闻监测系统 - Flask应用主入口
"""

import os
import sys

from flask import Flask, render_template, request, jsonify, send_file

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入项目模块
from config.config_manager import ConfigManager
from core.database import DatabaseManager
from core.rss_parser import RSSParser
from core.llm_analyzer import LLMAnalyzer
from core.rss_source_manager import get_all_rss_sources
from services.news_service import NewsService
from services.rss_service import RSSService
from services.analysis_service import AnalysisService
from utils.loggers import get_logger, log_system_startup, log_system_shutdown
from utils.validators import validate_search_params, validate_export_params
from utils.exporters import export_news_data


class BlackSwanNewsApp:
    """黑天鹅新闻监测系统应用类"""
    
    def __init__(self):
        """初始化应用"""
        self.app = Flask(__name__)
        self.app.secret_key = os.getenv('FLASK_SECRET_KEY', 'black_swan_news_secret_key')
        
        # 初始化配置
        self.config_manager = ConfigManager()
        
        # 初始化数据库
        self.db = DatabaseManager()
        
        # 初始化服务
        self.news_service = NewsService(self.db)
        self.rss_service = RSSService(self.db)
        self.analysis_service = AnalysisService(self.db)
        
        # 初始化核心组件
        self.rss_parser = RSSParser(self.db)
        self.llm_analyzer = LLMAnalyzer()
        
        # 初始化日志
        self.logger = get_logger()
        
        # 设置路由
        self._setup_routes()
        
        # 设置错误处理
        self._setup_error_handlers()
    
    def _setup_routes(self):
        """设置应用路由"""
        
        # 页面路由
        @self.app.route('/')
        def index():
            """首页"""
            return render_template('index.html')
        
        @self.app.route('/news')
        def news_list():
            """新闻列表页"""
            return render_template('news_list.html')
        
        @self.app.route('/black-swan')
        def black_swan_news():
            """黑天鹅事件页"""
            return render_template('black_swan.html')
        
        @self.app.route('/sources')
        def sources():
            """数据源管理页"""
            return render_template('sources.html')
        
        @self.app.route('/about')
        def about():
            """关于页面"""
            return render_template('about.html')
        
        # API路由 - 新闻相关
        @self.app.route('/api/news')
        def api_get_news():
            """获取新闻列表API"""
            try:
                # 获取查询参数
                page = request.args.get('page', 1, type=int)
                per_page = request.args.get('per_page', 20, type=int)
                search = request.args.get('search', '')
                sort_by = request.args.get('sort_by', 'date_desc')
                black_swan_only = request.args.get('black_swan_only', 'false').lower() == 'true'
                event_type = request.args.get('event_type', '')
                risk_level = request.args.get('risk_level', '')
                time_range = request.args.get('time_range', '')
                source = request.args.get('source', '')
                
                # 验证参数
                params = {
                    'page': page,
                    'per_page': per_page,
                    'search': search,
                    'sort_by': sort_by,
                    'black_swan_only': black_swan_only,
                    'event_type': event_type,
                    'risk_level': risk_level,
                    'time_range': time_range,
                    'source': source
                }
                
                is_valid, message = validate_search_params(params)
                if not is_valid:
                    return jsonify({'error': message}), 400
                
                # 构建查询条件
                filters = {}
                if search:
                    filters['search'] = search
                if black_swan_only:
                    filters['black_swan_only'] = True
                if event_type:
                    filters['event_type'] = event_type
                if risk_level:
                    filters['risk_level'] = risk_level
                if time_range:
                    filters['time_range'] = time_range
                if source:
                    filters['source'] = source
                
                # 获取新闻数据
                news_data, total_count = self.news_service.get_news_paginated(
                    page=page,
                    per_page=per_page,
                    filters=filters,
                    sort_by=sort_by
                )
                
                # 计算分页信息
                total_pages = (total_count + per_page - 1) // per_page
                
                response_data = {
                    'news': news_data,
                    'pagination': {
                        'current_page': page,
                        'per_page': per_page,
                        'total_count': total_count,
                        'total_pages': total_pages,
                        'has_prev': page > 1,
                        'has_next': page < total_pages
                    }
                }
                
                return jsonify(response_data)
                
            except Exception as e:
                self.logger.error(f"获取新闻列表失败: {str(e)}")
                return jsonify({'error': '获取新闻列表失败'}), 500
        
        @self.app.route('/api/news/<news_id>')
        def api_get_news_detail(news_id):
            """获取新闻详情API"""
            try:
                news = self.news_service.get_news_by_id(news_id)
                if not news:
                    return jsonify({'error': '新闻不存在'}), 404
                
                return jsonify(news)
                
            except Exception as e:
                self.logger.error(f"获取新闻详情失败: {str(e)}")
                return jsonify({'error': '获取新闻详情失败'}), 500
        
        @self.app.route('/api/news/<news_id>/verify', methods=['POST'])
        def api_verify_news(news_id):
            """验证新闻事件API"""
            try:
                success = self.analysis_service.mark_as_verified(news_id)
                if success:
                    return jsonify({'message': '事件标记为已验证'})
                else:
                    return jsonify({'error': '标记验证失败'}), 400
                    
            except Exception as e:
                self.logger.error(f"验证新闻事件失败: {str(e)}")
                return jsonify({'error': '验证新闻事件失败'}), 500
        
        @self.app.route('/api/news/export')
        def api_export_news():
            """导出新闻数据API"""
            try:
                # 获取查询参数
                format_type = request.args.get('format', 'csv')
                include_analysis = request.args.get('include_analysis', 'true').lower() == 'true'
                start_date = request.args.get('start_date', '')
                end_date = request.args.get('end_date', '')
                event_type = request.args.get('event_type', '')
                risk_level = request.args.get('risk_level', '')
                source = request.args.get('source', '')
                
                # 验证参数
                params = {
                    'format': format_type,
                    'start_date': start_date,
                    'end_date': end_date,
                    'event_type': event_type,
                    'risk_level': risk_level,
                    'source': source
                }
                
                is_valid, message = validate_export_params(params)
                if not is_valid:
                    return jsonify({'error': message}), 400
                
                # 构建筛选条件
                filters = {}
                if start_date:
                    filters['start_date'] = start_date
                if end_date:
                    filters['end_date'] = end_date
                if event_type:
                    filters['event_type'] = event_type
                if risk_level:
                    filters['risk_level'] = risk_level
                if source:
                    filters['source'] = source
                
                # 获取所有符合条件的新闻
                news_data, _ = self.news_service.get_news_paginated(
                    page=1,
                    per_page=10000,  # 获取大量数据
                    filters=filters
                )
                
                if not news_data:
                    return jsonify({'error': '没有数据可导出'}), 404
                
                # 导出数据
                if format_type == 'json':
                    content, filename = export_news_data(news_data, 'json', include_analysis)
                    return send_file(
                        content.encode('utf-8'),
                        as_attachment=True,
                        download_name=filename,
                        mimetype='application/json'
                    )
                else:
                    content, filename = export_news_data(news_data, 'csv', include_analysis)
                    return send_file(
                        content.encode('utf-8'),
                        as_attachment=True,
                        download_name=filename,
                        mimetype='text/csv'
                    )
                    
            except Exception as e:
                self.logger.error(f"导出新闻数据失败: {str(e)}")
                return jsonify({'error': '导出新闻数据失败'}), 500
        
        # API路由 - 统计相关
        @self.app.route('/api/stats')
        def api_get_stats():
            """获取系统统计API"""
            try:
                stats = self.news_service.get_system_stats()
                return jsonify(stats)
                
            except Exception as e:
                self.logger.error(f"获取系统统计失败: {str(e)}")
                return jsonify({'error': '获取系统统计失败'}), 500
        
        @self.app.route('/api/stats/black-swan')
        def api_get_black_swan_stats():
            """获取黑天鹅事件统计API"""
            try:
                stats = self.analysis_service.get_black_swan_stats()
                return jsonify(stats)
                
            except Exception as e:
                self.logger.error(f"获取黑天鹅统计失败: {str(e)}")
                return jsonify({'error': '获取黑天鹅统计失败'}), 500
        
        @self.app.route('/api/stats/sources')
        def api_get_sources_stats():
            """获取数据源统计API"""
            try:
                stats = {
                'total_sources': 0,
                'enabled_sources': 0,
                'disabled_sources': 0,
                'total_news': 0,
                'sources': []
            }
                return jsonify(stats)
                
            except Exception as e:
                self.logger.error(f"获取数据源统计失败: {str(e)}")
                return jsonify({'error': '获取数据源统计失败'}), 500
        
        # API路由 - 数据源相关
        @self.app.route('/api/sources')
        def api_get_sources():
            """获取数据源列表API"""
            try:
                # 直接从RSS源管理器获取数据源
                sources = get_all_rss_sources()
                # 转换为字典列表以便JSON序列化
                sources_data = [source.to_dict() for source in sources]
                return jsonify(sources_data)
                
            except Exception as e:
                self.logger.error(f"获取数据源列表失败: {str(e)}")
                return jsonify({'error': '获取数据源列表失败'}), 500
        
        @self.app.route('/api/sources', methods=['POST'])
        def api_add_source():
            """添加数据源API"""
            try:
                data = request.get_json()
                if not data:
                    return jsonify({'error': '请求数据不能为空'}), 400
                
                source_id = self.rss_service.add_source(data)
                if source_id:
                    return jsonify({'message': '数据源添加成功', 'id': source_id})
                else:
                    return jsonify({'error': '数据源添加失败'}), 400
                    
            except Exception as e:
                self.logger.error(f"添加数据源失败: {str(e)}")
                return jsonify({'error': '添加数据源失败'}), 500
        
        @self.app.route('/api/sources/<source_id>', methods=['PATCH'])
        def api_update_source(source_id):
            """更新数据源API"""
            try:
                data = request.get_json()
                if not data:
                    return jsonify({'error': '请求数据不能为空'}), 400
                
                success = self.rss_service.update_source(source_id, data)
                if success:
                    return jsonify({'message': '数据源更新成功'})
                else:
                    return jsonify({'error': '数据源更新失败'}), 400
                    
            except Exception as e:
                self.logger.error(f"更新数据源失败: {str(e)}")
                return jsonify({'error': '更新数据源失败'}), 500
        
        @self.app.route('/api/sources/<source_id>/test', methods=['POST'])
        def api_test_source(source_id):
            """测试数据源API"""
            try:
                success, message = self.rss_parser.test_source(source_id)
                if success:
                    return jsonify({'success': True, 'message': message})
                else:
                    return jsonify({'success': False, 'message': message}), 400
                    
            except Exception as e:
                self.logger.error(f"测试数据源失败: {str(e)}")
                return jsonify({'success': False, 'message': '测试数据源失败'}), 500
        
        # API路由 - 系统操作
        @self.app.route('/api/system/fetch-news', methods=['POST'])
        def api_fetch_news():
            """手动抓取新闻API"""
            try:
                success_count, total_count = self.rss_parser.fetch_all_sources()
                return jsonify({
                    'message': f'成功抓取 {success_count}/{total_count} 个数据源',
                    'success_count': success_count,
                    'total_count': total_count
                })
                
            except Exception as e:
                self.logger.error(f"手动抓取新闻失败: {str(e)}")
                return jsonify({'error': '手动抓取新闻失败'}), 500
        
        @self.app.route('/api/system/analyze-news', methods=['POST'])
        def api_analyze_news():
            """手动分析新闻API"""
            try:
                count = request.args.get('count', 10, type=int)
                analyzed_count = self.analysis_service.analyze_recent_news(count)
                return jsonify({
                    'message': f'成功分析 {analyzed_count} 条新闻',
                    'analyzed_count': analyzed_count
                })
                
            except Exception as e:
                self.logger.error(f"手动分析新闻失败: {str(e)}")
                return jsonify({'error': '手动分析新闻失败'}), 500
        
        # API路由 - 日志相关
        @self.app.route('/api/logs/sources')
        def api_get_source_logs():
            """获取数据源操作日志API"""
            try:
                # 这里可以返回最近的数据源操作日志
                # 暂时返回空数组，实际实现需要从数据库查询
                logs = []
                return jsonify(logs)
                
            except Exception as e:
                self.logger.error(f"获取数据源日志失败: {str(e)}")
                return jsonify({'error': '获取数据源日志失败'}), 500
    
    def _setup_error_handlers(self):
        """设置错误处理"""
        
        @self.app.errorhandler(404)
        def not_found(error):
            """404错误处理"""
            if request.path.startswith('/api/'):
                return jsonify({'error': '接口不存在'}), 404
            return render_template('404.html'), 404
        
        @self.app.errorhandler(500)
        def internal_error(error):
            """500错误处理"""
            self.logger.error(f"服务器内部错误: {str(error)}")
            if request.path.startswith('/api/'):
                return jsonify({'error': '服务器内部错误'}), 500
            return render_template('500.html'), 500
        
        @self.app.errorhandler(400)
        def bad_request(error):
            """400错误处理"""
            if request.path.startswith('/api/'):
                return jsonify({'error': '请求参数错误'}), 400
            return render_template('400.html'), 400
    
    def initialize_database(self):
        """初始化数据库"""
        try:
            # DatabaseManager在构造函数中已经初始化数据库
            # 这里只需要验证数据库连接
            self.db.get_connection()
            self.logger.info("数据库初始化完成")
        except Exception as e:
            self.logger.error(f"数据库初始化失败: {str(e)}")
            raise
    
    def start_scheduler(self):
        """启动定时任务"""
        try:
            # 导入并启动调度器
            from core.scheduler import start_scheduler
            start_scheduler(self.config_manager)
            self.logger.info("定时任务调度器已启动")
        except Exception as e:
            self.logger.error(f"启动定时任务调度器失败: {str(e)}")
    
    def run(self, host='0.0.0.0', port=5000, debug=False):
        """
        运行Flask应用
        
        Args:
            host: 主机地址
            port: 端口号
            debug: 是否启用调试模式
        """
        try:
            # 记录系统启动
            log_system_startup()
            
            # 初始化数据库
            self.initialize_database()
            
            # 启动定时任务
            self.start_scheduler()
            
            self.logger.info(f"黑天鹅新闻监测系统启动成功 - http://{host}:{port}")
            
            # 运行Flask应用
            self.app.run(host=host, port=port, debug=debug)
            
        except Exception as e:
            self.logger.error(f"应用启动失败: {str(e)}")
            raise
        finally:
            # 记录系统关闭
            log_system_shutdown()


def create_app():
    """创建Flask应用实例（用于生产部署）"""
    app_instance = BlackSwanNewsApp()
    return app_instance.app


if __name__ == '__main__':
    # 创建并运行应用
    app = BlackSwanNewsApp()
    app.run(
        host=os.getenv('FLASK_HOST', '0.0.0.0'),
        port=int(os.getenv('FLASK_PORT', 5550)),
        debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    )