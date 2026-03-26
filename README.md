# 🚀 Stock Analysis AI

🤖 **OpenClaw Ready** | 🇬🇧 English | 🇨🇳 [中文](./README.zh.md)

👉 **[Live AI Stock Review Blog](https://donvink.github.io/stock-review/)** 

📖 **[Developer Quick Start for OpenClaw](./skills/stock_review/SKILL.md)**

An automated stock market analysis system powered by **Gemini**, with native **OpenClaw** support. This project leverages Linux-driven automation to fetch global market data, generate deep A-股 (A-share) daily reviews, and sync them seamlessly to both a **[Hugo blog](https://donvink.github.io/stock-review/)** and **WeChat Official Account**.

---

## Prerequisites

- Node.js environment installed
- Ability to run `npx bun` commands

---

## ⚡ Quick Start

Depending on your use case, choose one of the following methods to get started:

### 🤖 For OpenClaw Users (Agent Integration)
This project is a standardized **OpenClaw Skill**. You can install it directly into your AI Agent environment:

* **Option 1: Quick Install**
    ```bash
    npx skills add Donvink/stock-review
    ```

* **Option 2: Skill Specification**
  Simply tell OpenClaw:

  ```bash
  Please install Skills from github.com/Donvink/stock-review
  ```

  For detailed API schemas and agent-calling conventions, see: 👉 **[OpenClaw Skill Guide & Specification](./skills/stock_review/SKILL.md)**


* **Option 3: Install from ClawHub**

  ```bash
  clawhub install stock-review-ai
  ```


### 💻 For Developers (Standalone Setup)
If you want to run the analysis engine manually or contribute to the code:

1. **Clone & Setup**
   ```bash
   git clone https://github.com/Donvink/stock-review.git
   cd stock-review
   pip install -r requirements.txt
   ```

2. **Configure Secrets**

   Set your `GEMINI_API_KEY` in a `.env` file.


4. **Run Analysis**
   ```bash
   python skills/stock_review/scripts/main.py
   ```

---

## 📸 Project Overview

### 🖥️ Homepage Overview
![Main Dashboard](./imgs/overview.jpg) 

### 📂 Report Directory
![Table of Contents](./imgs/contents.jpg)

### 📈 AI Analysis Example
The system generates multi-dimensional reports based on real-time market data. You can view a full sample report generated on March 4, 2026, here:

👉 **[View Sample AI Report: March 4, 2026](https://github.com/Donvink/stock-review/blob/main/data/20260304/ai_analysis_20260304.md)**

**Key Insights from this Report:**

* **Market Sentiment Diagnosis**: Provides a quantitative health check of the market based on advancing/declining stocks.
* **Core Themes**: Identifies leading sectors like AI, Digital Economy, and Computing Power.
* **Price Action Analysis**: Tracks key stocks and their limit-up performance.
* **Trading Strategy**: Offers specific entry and exit suggestions for the following trading session.

![AI Insights](./imgs/report.jpg)


### Dashboard Preview

The frontend is built with **Hugo**, providing a clean and intuitive archive of all historical market analyses.


### Report Structure

Each report is meticulously organized, covering market snapshots, sector analysis, limit-up gradients, and AI-driven insights.


---

## 📊 AI Analysis Report Examples

The system generates multi-dimensional reports based on real-time data from **AkShare**:

* **Market Sentiment Diagnosis**: Quantitatively analyzes the advance-decline ratio and limit-up counts to identify the current market cycle stage.
* **Core Themes & Capital Flow**: Identifies the strongest leading sectors and the direction of net capital inflows for the day.
* **Limit-Up Gradients & Price Action**: Tracks the highest-ranking stocks (space-setters) and analyzes market benchmarks.
* **Next-Day Trading Strategy**: Provides defensive and offensive pivot points based on historical data models and AI logic.

---

## 🛠️ Deployment & Workflow

This program is optimized for **WSL (Ubuntu)** or **Linux Servers**, and fully supports **GitHub Actions** for CI/CD automation.

### 1. Clone the Repository

```bash
git clone https://github.com/Donvink/stock-review.git
cd stock-review

```

### 2. Environment Setup

Ensure you have Python 3.10+ installed. Install the required dependencies:

```bash
pip install -r requirements.txt

```

### 3. Configuration (Environment Variables)

For security, never hardcode your keys. Use a `.env` file locally or configure **GitHub Secrets**:

* `GEMINI_API_KEY`: Your Google AI API key.
* `WECHAT_APP_ID`: Your WeChat Official Account AppID.
* `WECHAT_APP_SECRET`: Your WeChat Official Account AppSecret.

### 4. Execution

**To manually generate and upload a report:**

```bash
cd skills/stock_review/scripts
python main.py

```

---

## 🤖 Automation with GitHub Actions

The project is fully automated using GitHub Actions. It is configured to run daily to capture the market close data.

### Scheduled Execution (Cron)

The workflow is set to trigger automatically every trading day at **21:00 Beijing Time (13:00 UTC)**.

```yaml
# .github/workflows/main.yml
on:
  schedule:
    # 08:00 UTC is 16:00 Beijing Time
    - cron: '0 13 * * 1-5' 
  workflow_dispatch: # Allows manual triggering

```

### Automated Workflow Steps

1. **Data Fetching**: Pulls the latest A-share market data via AkShare.
2. **AI Analysis**: Gemini 3 Flash generates the review text in Markdown format.
3. **Blog Deployment**: Submits the Markdown to the Hugo content directory and redeploys the site to [donvink.github.io/stock-review/](https://donvink.github.io/stock-review/).
4. **WeChat Integration**: Converts Markdown to styled HTML and uploads it to the WeChat Draft Box.

---

## 💡 Pro Tips

* **IP Whitelist**: Remember to add your server's IP (or GitHub Actions runner IP) to the WeChat API Whitelist in the developer settings.


---

## 💡 Implementation Note

* **Timezone Offset**: GitHub Actions uses UTC. Beijing Time (CST) is **UTC+8**.
* **Cron Syntax**: `'0 8 * * 1-5'` represents:
* `0`: Minute 0
* `8`: Hour 8 (UTC)
* `* *`: Every day of the month
* `1-5`: Monday through Friday (trading days)

---

**If you find this project helpful, please give it a Star! ⭐**
