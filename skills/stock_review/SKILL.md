---
name: stock-review
description: A股市场自动化复盘分析系统，基于Gemini AI生成每日市场洞察报告，支持发布到Hugo博客和微信公众号
version: 1.0.0
metadata:
  openclaw:
    homepage: https://github.com/donvink/stock-review
    requires:
      anyBins:
        - python3
        - python
---

# 🚀 Stock Review

👉 **[在线演示博客](https://donvink.github.io/stock-review/)**

## Language

**匹配用户语言**: 使用用户使用的相同语言回复。如果用户用中文写，就用中文回复。如果用户用英文写，就用英文回复。

## 脚本目录

**Agent执行**: 确定此SKILL.md目录为 `{baseDir}`，然后使用 `{baseDir}/scripts/<name>.py`。运行时需确保Python 3.10+已安装，依赖包已配置。

| 脚本 | 用途 |
|------|------|
| `scripts/fetch_data.py` | 获取A股市场数据（指数、个股、板块等） |
| `scripts/analyze.py` | Gemini AI分析市场数据 |
| `scripts/post_to_hugo.py` | 发布到Hugo博客 |
| `scripts/post_to_wechat.py` | 发布到微信公众号 |
| `scripts/main.py` | 主执行脚本，协调整个流程 |
| `scripts/check_env.py` | 验证环境和权限 |

## 偏好配置 (EXTEND.md)

检查EXTEND.md是否存在（优先级顺序）：

```bash
# macOS, Linux, WSL, Git Bash
test -f .baoyu-skills/stock-review/EXTEND.md && echo "project"
test -f "${XDG_CONFIG_HOME:-$HOME/.config}/baoyu-skills/stock-review/EXTEND.md" && echo "xdg"
test -f "$HOME/.baoyu-skills/stock-review/EXTEND.md" && echo "user"
```

```powershell
# PowerShell (Windows)
if (Test-Path .baoyu-skills/stock-review/EXTEND.md) { "project" }
$xdg = if ($env:XDG_CONFIG_HOME) { $env:XDG_CONFIG_HOME } else { "$HOME/.config" }
if (Test-Path "$xdg/baoyu-skills/stock-review/EXTEND.md") { "xdg" }
if (Test-Path "$HOME/.baoyu-skills/stock-review/EXTEND.md") { "user" }
```

┌─────────────────────────────────────────────────┬───────────────────┐
│                       Path                       │     Location      │
├─────────────────────────────────────────────────┼───────────────────┤
│ .baoyu-skills/stock-review/EXTEND.md             │ Project directory │
├─────────────────────────────────────────────────┼───────────────────┤
│ $HOME/.baoyu-skills/stock-review/EXTEND.md       │ User home         │
└─────────────────────────────────────────────────┴───────────────────┘

┌───────────┬───────────────────────────────────────────────────────────────────────────┐
│  Result   │                                  Action                                   │
├───────────┼───────────────────────────────────────────────────────────────────────────┤
│ Found     │ Read, parse, apply settings                                               │
├───────────┼───────────────────────────────────────────────────────────────────────────┤
│ Not found │ 使用默认配置继续                                                          │
└───────────┴───────────────────────────────────────────────────────────────────────────┘

**EXTEND.md支持**: 默认发布平台 | 默认是否跳过AI分析 | 默认数据回溯天数 | 默认请求延迟 | 默认重试次数 | API密钥配置

**最小支持键** (不区分大小写，接受 `1/0` 或 `true/false`):

| 键 | 默认值 | 说明 |
|-----|---------|------|
| `default_platforms` | `["hugo"]` | 默认发布平台 (`hugo`/`wechat`/`both`) |
| `default_skip_ai` | `false` | 默认是否跳过AI分析 |
| `default_backtrack_days` | `20` | 默认数据回溯天数 |
| `default_request_delay` | `0.5` | 默认请求延迟(秒) |
| `default_max_retries` | `3` | 默认重试次数 |
| `gemini_api_key` | - | Gemini API密钥 |
| `wechat_app_id` | - | 微信公众号AppID |
| `wechat_app_secret` | - | 微信公众号AppSecret |

**推荐EXTEND.md示例**:

```md
default_platforms: both
default_skip_ai: false
default_backtrack_days: 20
default_request_delay: 0.5
default_max_retries: 3
gemini_api_key: your_gemini_api_key_here
wechat_app_id: your_wechat_app_id
wechat_app_secret: your_wechat_appsecret
```

**值优先级**:
1. CLI参数
2. EXTEND.md配置
3. Skill默认值

## 环境检查 (可选)

首次使用前，建议运行环境检查。用户可以跳过此步骤。

```bash
python3 {baseDir}/scripts/check_env.py
```

检查项: Python版本 | 依赖包 | API密钥 | 网络连接 | 目录权限

**如果任何检查失败**，提供修复指导：

| 检查项 | 修复方法 |
|-------|----------|
| Python版本 | 安装Python 3.10+：`brew install python@3.10` (macOS) 或 `apt install python3.10` (Linux) |
| 依赖包 | 运行 `pip install -r {baseDir}/requirements.txt` |
| Gemini API密钥 | 在EXTEND.md中设置或通过CLI参数传递 |
| 微信公众号凭证 | 在EXTEND.md中设置（可选） |
| 网络连接 | 检查网络代理设置 |
| 目录权限 | 确保data/和content/posts/目录可写 |

## 工作流程概览

复制此清单并随进度勾选：

```
复盘分析进度:
- [ ] 步骤0: 加载偏好配置 (EXTEND.md)
- [ ] 步骤1: 确定执行参数
- [ ] 步骤2: 获取市场数据
- [ ] 步骤3: 运行AI分析（可选）
- [ ] 步骤4: 生成报告
- [ ] 步骤5: 发布到平台
- [ ] 步骤6: 报告完成
```

### 步骤0: 加载偏好配置

检查并加载EXTEND.md设置（见上方偏好配置部分）。

解析并存储以下默认值供后续步骤使用：
- `default_platforms` (默认 `["hugo"]`)
- `default_skip_ai` (默认 `false`)
- `default_backtrack_days` (默认 `20`)
- `default_request_delay` (默认 `0.5`)
- `default_max_retries` (默认 `3`)

### 步骤1: 确定执行参数

| 参数 | 来源 | 说明 |
|------|------|------|
| `--date` | CLI参数 | 指定日期 (YYYYMMDD)，默认自动获取最新 |
| `--force` | CLI参数 | 强制刷新数据 |
| `--skip-ai` | CLI参数/EXTEND.md | 跳过AI分析 |
| `--platform` | CLI参数/EXTEND.md | 发布平台 (`hugo`/`wechat`/`both`) |
| `--validate` | CLI参数 | 仅验证配置 |

**示例**:
```bash
python3 {baseDir}/scripts/main.py --date 20260304 --platform both --force
```

### 步骤2: 获取市场数据

根据指定日期获取以下数据：

| 数据类型 | 来源 | 文件 |
|----------|------|------|
| 指数数据 | stock_zh_index_spot_sina | `data/{date}/index_{date}.csv` |
| 涨停池 | stock_zt_pool_em | `data/{date}/zt_pool_{date}.csv` |
| 跌停池 | stock_zt_pool_dtgc_em | `data/{date}/dt_pool_{date}.csv` |
| 炸板池 | stock_zt_pool_zbgc_em | `data/{date}/zb_pool_{date}.csv` |
| 全市场数据 | stock_zh_a_spot_em | `data/{date}/A_stock_{date}.csv` |
| 成交额前20 | 计算得出 | `data/{date}/top_amount_stocks_{date}.csv` |
| 概念板块 | stock_board_concept_name_em | `data/{date}/concept_summary_{date}.csv` |
| 龙虎榜 | stock_lhb_detail_daily_sina | `data/{date}/lhb_{date}.csv` |
| Watchlist | 计算得出 | `data/{date}/watchlist*_{date}.csv` |

**重试机制**:
- 默认重试3次
- 请求间隔0.5秒
- 失败自动切换备用接口

### 步骤3: 运行AI分析

**CRITICAL**: 仅在以下情况运行AI分析：
- `--skip-ai` 未设置
- `GEMINI_API_KEY` 已配置（通过EXTEND.md或环境变量）

**AI分析提示词**:

```python
prompt = f"""
角色设定：你是一位拥有20年经验的A股资深策略分析师...

任务描述：基于【当日复盘数据】进行多维度复盘：
1. 🚩 市场情绪诊断
2. 💰 核心主线与资金流向
3. 🪜 连板梯度与空间博弈
4. ⚡ 重点异动个股分析
5. 🧭 次日交易策略建议

📊 当日复盘数据:
{market_summary}
"""
```

**输出**: `data/{date}/ai_analysis_{date}.md`

### 步骤4: 生成报告

**市场汇总报告**:
- 文件: `data/{date}/market_summary_{date}.md`
- 格式: Markdown
- 内容: 所有数据的表格化汇总

**AI分析报告** (如果运行):
- 文件: `data/{date}/ai_analysis_{date}.md`
- 格式: Markdown
- 内容: Gemini生成的深度分析

### 步骤5: 发布到平台

**Hugo博客发布**:

```bash
python3 {baseDir}/scripts/post_to_hugo.py --market-summary <file> --ai-analysis <file> --date <date>
```

**输出**: `content/posts/stock-analysis-{YYYY-MM-DD}.md`

**微信公众号发布** (需要API凭证):

```bash
python3 {baseDir}/scripts/post_to_wechat.py --market-summary <file> --ai-analysis <file> --date <date>
```

**微信公众号API请求规则**:
- 端点: `POST https://api.weixin.qq.com/cgi-bin/draft/add?access_token=ACCESS_TOKEN`
- `article_type`: `news`
- 需要 `thumb_media_id` (封面图)
- 评论设置: `need_open_comment=1`, `only_fans_can_comment=0`

### 步骤6: 完成报告

**成功执行后报告**:

```
✅ A股复盘分析完成！

日期: 2026-03-04
数据: data/20260304/ (12个文件)
AI分析: ✓ 已生成 (Gemini 2.0 Flash)

发布平台:
→ Hugo博客: content/posts/stock-analysis-2026-03-04.md
→ 微信公众号: 草稿ID: abc123def456

市场快照:
• 上证指数: 3350.52 (+1.02%)
• 成交额: 1.95万亿
• 涨跌比: 2857 / 2058
• 涨停/跌停: 78 / 3

查看博客: https://donvink.github.io/stock-review/
```

**错误时报告**:

```
❌ 复盘分析失败

错误: 无法获取涨停板数据
建议: 
1. 检查网络连接
2. 尝试 --force 参数强制刷新
3. 增加 --date 指定其他日期
```

## 详细功能说明

### 数据获取模块

| 函数 | 用途 | 重试 | 缓存 |
|------|------|------|------|
| `stock_summary()` | 获取指数数据 | ✓ | ✓ |
| `stock_zt_dt_pool()` | 获取涨跌停数据 | ✓ | ✓ |
| `fetch_all_stock_data()` | 获取全市场数据 | ✓ (3次) | ✓ |
| `get_top_amount_stocks()` | 获取成交额前20 | ✓ | ✓ |
| `get_concept_summary()` | 获取概念板块 | ✓ | ✓ |
| `get_lhb_data()` | 获取龙虎榜 | ✓ | ✓ |

### AI分析模块

**模型**: `gemini-2.0-flash-exp`

**分析维度**:
1. **市场情绪诊断** - 涨跌比、涨停跌停对比、成交额
2. **核心主线追踪** - 资金流向、热点板块
3. **连板梯度分析** - 空间板高度、连板结构
4. **异动个股分析** - 大额成交、龙虎榜
5. **次日策略建议** - 基于数据的操作建议

### 发布模块

| 平台 | 方式 | 要求 | 输出 |
|------|------|------|------|
| Hugo博客 | 文件写入 | 无 | Markdown文件 |
| 微信公众号 | API | AppID/Secret | 草稿ID |

## 功能对比

| 功能 | 数据获取 | AI分析 | Hugo发布 | 微信发布 |
|------|----------|--------|----------|----------|
| 自动获取最新日期 | ✓ | - | - | - |
| 数据缓存 | ✓ | - | - | - |
| 重试机制 | ✓ | - | - | - |
| 多源备份 | ✓ | - | - | - |
| 格式化数值(亿/万) | ✓ | - | - | - |
| 过滤ST股票 | ✓ | - | - | - |
| Watchlist构建 | ✓ | - | - | - |
| 市场情绪诊断 | - | ✓ | - | - |
| 连板梯度分析 | - | ✓ | - | - |
| 策略建议 | - | ✓ | - | - |
| Markdown格式 | - | ✓ | ✓ | ✓ |
| 时区处理 | - | - | ✓ | - |
| Hugo frontmatter | - | - | ✓ | - |
| 微信HTML转换 | - | - | - | ✓ |
| 评论设置 | - | - | - | ✓ |

## 先决条件

**必需**:
- Python 3.10+
- 依赖包: `pip install -r requirements.txt`
- Gemini API密钥（用于AI分析）

**可选**:
- 微信公众号AppID和AppSecret（用于微信发布）
- Hugo博客环境（用于博客发布）

**配置位置** (优先级):
1. CLI参数
2. EXTEND.md (项目级/用户级)
3. 环境变量
4. 默认值

## 故障排除

| 问题 | 解决方案 |
|------|----------|
| 无法获取数据 | 检查网络，尝试 `--force`，使用 `--date` 指定其他日期 |
| Gemini API错误 | 检查API密钥是否有效，配额是否足够 |
| 涨停板数据为空 | 可能是非交易日，尝试回溯其他日期 |
| 微信发布失败 | 检查AppID/Secret，确认IP已加入白名单 |
| 中文乱码 | 确保文件编码为UTF-8 |
| 数据格式错误 | 检查CSV文件，确认代码列未转为数字 |
| 超时错误 | 增加 `default_request_delay` 或 `default_max_retries` |
| 内存不足 | 减少数据量，或分批处理 |

## 扩展支持

通过EXTEND.md自定义配置。参见**偏好配置**部分了解路径和支持的选项。

## 相关参考

| 主题 | 参考 |
|------|------|
| AkShare文档 | https://www.akshare.xyz/ |
| Gemini API | https://ai.google.dev/ |
| 微信公众号API | https://developers.weixin.qq.com/doc/offiaccount/ |
| Hugo文档 | https://gohugo.io/ |

## 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 1.0.0 | 2026-03-11 | 初始版本 |


