# 🚀 Stock Analysis AI (股市分析 AI)

🤖 **支持 OpenClaw** | 🇬🇧 [English](./README.md) | 🇨🇳 中文

👉 **[在线 AI 股市复盘博客](https://donvink.github.io/stock-review/)** 

📖 **[OpenClaw 开发者快速入门指南](./skills/stock_review/SKILL.md)**

这是一个由 **Gemini** 驱动的自动化股市分析系统，原生支持 **OpenClaw**。本项目利用基于 Linux 的自动化流程获取全球市场数据，生成深度的 **A 股** 每日复盘报告，并无缝同步至 **[Hugo 博客](https://donvink.github.io/stock-review/)** 和 **微信公众号**。

---

## 前置要求

- 已安装 Node.js 环境
- 能够运行 `npx bun` 命令

---

## ⚡ 快速开始

根据您的使用场景，选择以下方法之一开始：

### 🤖 针对 OpenClaw 用户 (智能体集成)
本项目是一个标准化的 **OpenClaw Skill**。您可以直接将其安装到您的 AI Agent 环境中：

* **选项 1：快速安装**
    ```bash
    npx skills add Donvink/stock-review
    ```

* **选项 2：通过指令安装**
    只需告诉 OpenClaw：
    > 请安装来自 [github.com/Donvink/stock-review](https://github.com/Donvink/stock-review) 的 Skills

    有关详细的 API Schema 和 Agent 调用约定，请参阅：👉 **[OpenClaw Skill 指南与规范](./skills/stock_review/SKILL.md)**


* **选项 3：通过 ClawHub 安装**
    ```bash
    clawhub install stock-review-ai
    ```


### 💻 针对开发者 (独立运行)
如果您想手动运行分析引擎或贡献代码：

1. **克隆与设置**
   ```bash
   git clone https://github.com/Donvink/stock-review.git
   cd stock-review
   pip install -r requirements.txt
   ```

2. **配置密钥**
   在 `.env` 文件中设置您的 `GEMINI_API_KEY`。

3. **执行分析**
   ```bash
   python skills/stock_review/scripts/main.py
   ```

---

## 📸 项目概览

### 🖥️ 首页展示
![主面板](./imgs/overview.jpg) 

### 📂 报告归档
![目录](./imgs/contents.jpg)

### 📈 AI 分析示例
系统根据实时市场数据生成多维度报告。您可以点击下方链接查看 2026 年 3 月 4 日生成的完整示例报告：

👉 **[查看 AI 报告样本：2026年3月4日](https://github.com/Donvink/stock-review/blob/main/data/20260304/ai_analysis_20260304.md)**

**本报告的核心洞察：**

* **市场情绪诊断**：基于涨跌家数对市场健康状况进行定量体检。
* **核心题材**：识别 AI、数字经济、算力等领涨板块。
* **个股表现分析**：跟踪核心标的及其涨停表现。
* **交易策略**：为下一个交易日提供具体的进场和出场建议。

![AI 洞察](./imgs/report.jpg)


### 博客预览
前端基于 **Hugo** 构建，提供所有历史市场分析的简洁直观的归档页面。

### 报告结构
每份报告都经过精心组织，涵盖市场快照、板块分析、涨停梯队及 AI 驱动的深度见解。

---

## 📊 AI 分析报告示例

系统基于来自 **AkShare** 的实时数据生成多维度报告：

* **市场情绪诊断**：定量分析涨跌比和涨停家数，识别当前市场周期阶段。
* **核心题材与资金流向**：识别当日最强领涨板块及净资金流入方向。
* **涨停梯队与连板表现**：跟踪最高标（空间板）并分析市场基准。
* **次日交易策略**：基于历史数据模型和 AI 逻辑提供防御性和进攻性的关键点位。

---

## 🛠️ 部署与工作流

本程序针对 **WSL (Ubuntu)** 或 **Linux 服务器** 进行了优化，并完全支持 **GitHub Actions** 进行 CI/CD 自动化。

### 1. 克隆仓库
```bash
git clone https://github.com/Donvink/stock-review.git
cd stock-review
```

### 2. 环境配置
确保已安装 Python 3.10+。安装所需依赖：
```bash
pip install -r requirements.txt
```

### 3. 配置（环境变量）
为了安全起见，请勿硬编码密钥。在本地使用 `.env` 文件，或在 **GitHub Secrets** 中配置：
* `GEMINI_API_KEY`: 您的 Google AI API 密钥。
* `WECHAT_APP_ID`: 您的微信公众号 AppID。
* `WECHAT_APP_SECRET`: 您的微信公众号 AppSecret。

### 4. 手动执行
**如需手动生成并上传报告：**
```bash
cd skills/stock_review/scripts
python main.py
```

---

## 🤖 GitHub Actions 自动化

本项目使用 GitHub Actions 实现完全自动化。配置为每天运行以捕捉收盘数据。

### 定时执行 (Cron)
工作流配置为在每个交易日的 **北京时间 21:00 (13:00 UTC)** 自动触发。

```yaml
# .github/workflows/main.yml
on:
  schedule:
    # 13:00 UTC 即北京时间 21:00
    - cron: '0 13 * * 1-5' 
  workflow_dispatch: # 支持手动触发
```

### 自动化工作流步骤
1. **数据获取**：通过 AkShare 拉取最新的 A 股市场数据。
2. **AI 分析**：Gemini 3 Flash 生成 Markdown 格式的复盘文字。
3. **博客部署**：将 Markdown 提交至 Hugo 内容目录，并重新部署站点至 [donvink.github.io/stock-review/](https://donvink.github.io/stock-review/)。
4. **微信集成**：将 Markdown 转换为美化的 HTML 并上传至微信草稿箱。

---

## 💡 专业提示

* **IP 白名单**：记得在微信公众号开发者设置中，将您的服务器 IP（或 GitHub Actions 运行器的 IP）添加到 IP 白名单中。

---

## 💡 实现细节备注

* **时区偏移**：GitHub Actions 使用 UTC 时间。北京时间 (CST) 为 **UTC+8**。
* **Cron 语法说明**：`'0 13 * * 1-5'` 表示：
  * `0`: 第 0 分钟
  * `13`: 13 时 (UTC)
  * `* *`: 每月每天
  * `1-5`: 周一至周五（交易日）

---
**如果您觉得这个项目对您有帮助，欢迎给个 Star! ⭐**