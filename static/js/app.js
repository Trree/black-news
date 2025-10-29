// 黑天鹅新闻网站前端JavaScript

document.addEventListener('DOMContentLoaded', function() {
    // 初始化应用
    initApp();
});

/**
 * 初始化应用
 */
function initApp() {
    // 绑定事件监听器
    bindEventListeners();
    
    // 加载统计数据
    loadStats();
    
    // 加载新闻列表
    loadNews();
}

/**
 * 绑定事件监听器
 */
function bindEventListeners() {
    // 搜索表单提交
    const searchForm = document.getElementById('searchForm');
    if (searchForm) {
        searchForm.addEventListener('submit', handleSearch);
    }
    
    // 筛选按钮点击
    const filterButtons = document.querySelectorAll('.filter-button');
    filterButtons.forEach(button => {
        button.addEventListener('click', handleFilter);
    });
    
    // 分页按钮点击
    document.addEventListener('click', function(e) {
        if (e.target.classList.contains('pagination-button') && !e.target.classList.contains('disabled')) {
            handlePagination(e.target);
        }
    });
    
    // 新闻卡片点击
    document.addEventListener('click', function(e) {
        const newsCard = e.target.closest('.news-card');
        if (newsCard) {
            const newsId = newsCard.dataset.newsId;
            if (newsId) {
                showNewsDetail(newsId);
            }
        }
    });
}

/**
 * 处理搜索
 */
function handleSearch(e) {
    e.preventDefault();
    const searchInput = document.getElementById('searchInput');
    const query = searchInput.value.trim();
    
    if (query) {
        loadNews({ search: query });
    } else {
        loadNews();
    }
}

/**
 * 处理筛选
 */
function handleFilter(e) {
    const button = e.currentTarget;
    const filterType = button.dataset.filter;
    
    // 更新活跃状态
    document.querySelectorAll('.filter-button').forEach(btn => {
        btn.classList.remove('active');
    });
    button.classList.add('active');
    
    // 根据筛选类型加载新闻
    switch(filterType) {
        case 'all':
            loadNews();
            break;
        case 'black-swan':
            loadNews({ black_swan_only: true });
            break;
        case 'recent':
            loadNews({ sort_by: 'date_desc' });
            break;
        case 'trending':
            loadNews({ sort_by: 'impact_desc' });
            break;
    }
}

/**
 * 处理分页
 */
function handlePagination(button) {
    const page = parseInt(button.dataset.page);
    const currentParams = getCurrentSearchParams();
    
    loadNews({ ...currentParams, page: page });
}

/**
 * 获取当前搜索参数
 */
function getCurrentSearchParams() {
    const params = {};
    const searchInput = document.getElementById('searchInput');
    const activeFilter = document.querySelector('.filter-button.active');
    
    if (searchInput && searchInput.value.trim()) {
        params.search = searchInput.value.trim();
    }
    
    if (activeFilter) {
        const filterType = activeFilter.dataset.filter;
        switch(filterType) {
            case 'black-swan':
                params.black_swan_only = true;
                break;
            case 'recent':
                params.sort_by = 'date_desc';
                break;
            case 'trending':
                params.sort_by = 'impact_desc';
                break;
        }
    }
    
    return params;
}

/**
 * 加载统计数据
 */
async function loadStats() {
    try {
        const response = await fetch('/api/stats');
        const data = await response.json();
        
        updateStatsDisplay(data);
    } catch (error) {
        console.error('加载统计数据失败:', error);
        showError('加载统计数据失败');
    }
}

/**
 * 更新统计数据显示
 */
function updateStatsDisplay(stats) {
    const statsContainer = document.getElementById('statsContainer');
    if (!statsContainer) return;
    
    statsContainer.innerHTML = `
        <div class="stat-card">
            <h3>总新闻数</h3>
            <div class="value">${stats.total_news || 0}</div>
        </div>
        <div class="stat-card">
            <h3>黑天鹅事件</h3>
            <div class="value">${stats.black_swan_news || 0}</div>
        </div>
        <div class="stat-card">
            <h3>活跃RSS源</h3>
            <div class="value">${stats.active_sources || 0}</div>
        </div>
        <div class="stat-card">
            <h3>今日更新</h3>
            <div class="value small">${stats.today_news || 0}</div>
        </div>
    `;
}

/**
 * 加载新闻列表
 */
async function loadNews(params = {}) {
    const newsContainer = document.getElementById('newsContainer');
    const loadingElement = document.getElementById('loadingIndicator');
    
    if (loadingElement) {
        loadingElement.style.display = 'block';
    }
    if (newsContainer) {
        newsContainer.innerHTML = '';
    }
    
    try {
        // 构建查询参数
        const queryParams = new URLSearchParams();
        Object.keys(params).forEach(key => {
            if (params[key] !== undefined && params[key] !== null) {
                queryParams.append(key, params[key]);
            }
        });
        
        const response = await fetch(`/api/news?${queryParams}`);
        const data = await response.json();
        
        updateNewsDisplay(data);
        updatePagination(data.pagination);
        
    } catch (error) {
        console.error('加载新闻失败:', error);
        showError('加载新闻失败');
    } finally {
        if (loadingElement) {
            loadingElement.style.display = 'none';
        }
    }
}

/**
 * 更新新闻显示
 */
function updateNewsDisplay(data) {
    const newsContainer = document.getElementById('newsContainer');
    if (!newsContainer) return;
    
    if (!data.news || data.news.length === 0) {
        newsContainer.innerHTML = `
            <div style="text-align: center; padding: 3rem; color: #7f8c8d;">
                <h3>暂无新闻数据</h3>
                <p>请检查RSS源配置或稍后再试</p>
            </div>
        `;
        return;
    }
    
    const newsHTML = data.news.map(news => createNewsCard(news)).join('');
    newsContainer.innerHTML = newsHTML;
}

/**
 * 创建新闻卡片HTML
 */
function createNewsCard(news) {
    const publishedDate = new Date(news.published_at).toLocaleDateString('zh-CN');
    const isBlackSwan = news.analysis_result && news.analysis_result.is_black_swan;
    const confidence = news.analysis_result ? Math.round(news.analysis_result.confidence * 100) : 0;
    
    return `
        <div class="news-card" data-news-id="${news.id}">
            <div class="news-card-header">
                <h3 class="news-card-title">${escapeHtml(news.title)}</h3>
                <div class="news-card-source">
                    <span class="news-card-source-name">${escapeHtml(news.source_name)}</span>
                    <span class="news-card-date">${publishedDate}</span>
                </div>
            </div>
            ${news.image_url ? `<img src="${news.image_url}" alt="${escapeHtml(news.title)}" class="news-card-image" onerror="this.style.display='none'">` : ''}
            <div class="news-card-content">
                <p class="news-card-summary">${escapeHtml(news.summary || '暂无摘要')}</p>
            </div>
            <div class="news-card-footer">
                <div class="news-card-meta">
                    ${isBlackSwan ? `<span class="black-swan-badge">黑天鹅事件 (${confidence}%)</span>` : '<span>常规新闻</span>'}
                </div>
                <a href="${news.url}" target="_blank" class="search-button" style="padding: 0.25rem 0.5rem; font-size: 0.9rem;">阅读原文</a>
            </div>
        </div>
    `;
}

/**
 * 更新分页控件
 */
function updatePagination(pagination) {
    const paginationContainer = document.getElementById('paginationContainer');
    if (!paginationContainer || !pagination) return;
    
    const { current_page, total_pages, has_prev, has_next } = pagination;
    
    let paginationHTML = '';
    
    // 上一页按钮
    paginationHTML += `
        <button class="pagination-button ${!has_prev ? 'disabled' : ''}" 
                data-page="${current_page - 1}" ${!has_prev ? 'disabled' : ''}>
            上一页
        </button>
    `;
    
    // 页码按钮
    for (let i = 1; i <= total_pages; i++) {
        if (i === 1 || i === total_pages || (i >= current_page - 2 && i <= current_page + 2)) {
            paginationHTML += `
                <button class="pagination-button ${i === current_page ? 'active' : ''}" 
                        data-page="${i}">
                    ${i}
                </button>
            `;
        } else if (i === current_page - 3 || i === current_page + 3) {
            paginationHTML += `<span>...</span>`;
        }
    }
    
    // 下一页按钮
    paginationHTML += `
        <button class="pagination-button ${!has_next ? 'disabled' : ''}" 
                data-page="${current_page + 1}" ${!has_next ? 'disabled' : ''}>
            下一页
        </button>
    `;
    
    paginationContainer.innerHTML = paginationHTML;
}

/**
 * 显示新闻详情
 */
async function showNewsDetail(newsId) {
    try {
        const response = await fetch(`/api/news/${newsId}`);
        const news = await response.json();
        
        // 创建模态框显示详情
        createNewsDetailModal(news);
    } catch (error) {
        console.error('加载新闻详情失败:', error);
        showError('加载新闻详情失败');
    }
}

/**
 * 创建新闻详情模态框
 */
function createNewsDetailModal(news) {
    const modal = document.createElement('div');
    modal.className = 'modal';
    modal.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background-color: rgba(0,0,0,0.7);
        display: flex;
        justify-content: center;
        align-items: center;
        z-index: 1000;
    `;
    
    const publishedDate = new Date(news.published_at).toLocaleDateString('zh-CN');
    const isBlackSwan = news.analysis_result && news.analysis_result.is_black_swan;
    const confidence = news.analysis_result ? Math.round(news.analysis_result.confidence * 100) : 0;
    
    modal.innerHTML = `
        <div class="modal-content" style="
            background: white;
            padding: 2rem;
            border-radius: 8px;
            max-width: 600px;
            max-height: 80vh;
            overflow-y: auto;
            position: relative;
        ">
            <button class="close-button" style="
                position: absolute;
                top: 1rem;
                right: 1rem;
                background: none;
                border: none;
                font-size: 1.5rem;
                cursor: pointer;
                color: #7f8c8d;
            ">&times;</button>
            
            <h2 style="margin-bottom: 1rem; color: #2c3e50;">${escapeHtml(news.title)}</h2>
            
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem; color: #7f8c8d;">
                <span>来源: ${escapeHtml(news.source_name)}</span>
                <span>发布时间: ${publishedDate}</span>
            </div>
            
            ${news.image_url ? `<img src="${news.image_url}" alt="${escapeHtml(news.title)}" style="width: 100%; height: auto; margin-bottom: 1rem; border-radius: 4px;">` : ''}
            
            <div style="margin-bottom: 1rem;">
                <h3 style="margin-bottom: 0.5rem; color: #2c3e50;">摘要</h3>
                <p style="line-height: 1.6;">${escapeHtml(news.summary || '暂无摘要')}</p>
            </div>
            
            ${news.analysis_result ? `
                <div style="margin-bottom: 1rem;">
                    <h3 style="margin-bottom: 0.5rem; color: #2c3e50;">AI分析结果</h3>
                    <div style="background: #f8f9fa; padding: 1rem; border-radius: 4px;">
                        <div style="display: flex; align-items: center; margin-bottom: 0.5rem;">
                            <span style="font-weight: 600;">类型: </span>
                            ${isBlackSwan ? 
                                `<span class="black-swan-badge" style="margin-left: 0.5rem;">黑天鹅事件</span>` : 
                                `<span style="margin-left: 0.5rem; color: #27ae60;">常规事件</span>`
                            }
                        </div>
                        <div style="margin-bottom: 0.5rem;">
                            <span style="font-weight: 600;">置信度: </span>
                            <span style="margin-left: 0.5rem;">${confidence}%</span>
                        </div>
                        ${news.analysis_result.reasoning ? `
                            <div>
                                <span style="font-weight: 600;">分析理由: </span>
                                <p style="margin-top: 0.5rem; line-height: 1.5;">${escapeHtml(news.analysis_result.reasoning)}</p>
                            </div>
                        ` : ''}
                    </div>
                </div>
            ` : ''}
            
            <div style="text-align: center; margin-top: 1.5rem;">
                <a href="${news.url}" target="_blank" class="search-button">阅读原文</a>
            </div>
        </div>
    `;
    
    // 关闭按钮事件
    const closeButton = modal.querySelector('.close-button');
    closeButton.addEventListener('click', () => {
        document.body.removeChild(modal);
    });
    
    // 点击背景关闭
    modal.addEventListener('click', (e) => {
        if (e.target === modal) {
            document.body.removeChild(modal);
        }
    });
    
    document.body.appendChild(modal);
}

/**
 * 显示错误消息
 */
function showError(message) {
    // 移除现有的错误消息
    const existingError = document.querySelector('.error-message');
    if (existingError) {
        existingError.remove();
    }
    
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error-message';
    errorDiv.textContent = message;
    errorDiv.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        z-index: 1001;
        max-width: 300px;
    `;
    
    document.body.appendChild(errorDiv);
    
    // 3秒后自动移除
    setTimeout(() => {
        if (errorDiv.parentNode) {
            errorDiv.parentNode.removeChild(errorDiv);
        }
    }, 3000);
}

/**
 * HTML转义函数
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * 显示成功消息
 */
function showSuccess(message) {
    const successDiv = document.createElement('div');
    successDiv.className = 'success-message';
    successDiv.textContent = message;
    successDiv.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        z-index: 1001;
        max-width: 300px;
    `;
    
    document.body.appendChild(successDiv);
    
    // 3秒后自动移除
    setTimeout(() => {
        if (successDiv.parentNode) {
            successDiv.parentNode.removeChild(successDiv);
        }
    }, 3000);
}

// 导出函数供其他脚本使用
window.BlackSwanNews = {
    loadNews,
    loadStats,
    showNewsDetail,
    showError,
    showSuccess
};