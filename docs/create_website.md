1. windows
install wsl
open wsl

2. prepare enviroment
# 更新系统包
sudo apt update

# 安装 Git
sudo apt install git

# 安装 Hugo (推荐安装 extended 扩展版，支持更多高级主题)
sudo apt install hugo

3. create Hugo project
# 创建一个名为 my_stock_site 的文件夹作为项目根目录
hugo new site my_stock_site

# 进入这个根目录
cd my_stock_site

# create script
touch analyze_stocks.py

4. initialize project
# 初始化 Git 仓库（安装主题必须先初始化 Git）
git init

# 添加 PaperMod 主题作为子模块
git submodule add --depth=1 https://github.com/adityatelange/hugo-PaperMod.git themes/PaperMod

# 将主题写入配置
# 注意：Hugo 现在默认使用 hugo.toml 而不是 config.toml
echo "theme = 'PaperMod'" >> hugo.toml

5. optimize config: change hugo.toml
"""
baseURL = 'https://yourdomain.com/'
languageCode = 'zh-cn'
title = 'AI 股市导航'
theme = "PaperMod"

[params]
    defaultTheme = "auto"
    # 在首页显示文章全文摘要，方便快速浏览资讯
    showFullContents = false 
    displayFullLangName = true
    
    [params.homeInfoParams]
        Title = "📈 每日股市 AI 分析"
        Content = "利用 AI 技术自动抓取并分析全球市场资讯。由 Linux 自动化驱动。"

    [params.assets]
        disableHLJS = false # 开启代码高亮（如果你要展示分析代码）

    # 导航栏配置
    [[menu.main]]
        identifier = "posts"
        name = "资讯档案"
        url = "/posts/"
        weight = 10
"""

6. preview
hugo server -D

7. get market information



更新theme（更新submodule）
git submodule update --init --recursive
hugo server --baseURL http://localhost:1313/ --bind 0.0.0.0