1. 在 GitHub 上新建项目
登录你的 GitHub，点击右上角的 + -> New repository。

填写仓库名称（例如：my-stock-data）。

勾选 Add a README file（这样可以方便你直接在网页端修改代码）。

点击 Create repository。

2. 配置仓库权限 (关键步骤)
GitHub Actions 默认可能没有权限把生成的文件推送到你的仓库。

进入仓库的 Settings -> Actions -> General。

拉到最下方找到 Workflow permissions。

选择 Read and write permissions。

点击 Save。

3. 创建文件结构

4. 在 GitHub 仓库中配置 Secret（关键步骤）
千万不要直接把这串字符写在 Python 脚本里，否则别人看你的 GitHub 代码就能盗用你的积分。请按照以下步骤操作：

打开你的 GitHub 项目页面。

点击顶部导航栏的 Settings（设置）。

在左侧菜单栏找到 Security 部分，点击 Secrets and variables -> Actions。

点击右侧绿色的 New repository secret 按钮。

在 Name 框中输入：TUSH_TOKEN（必须全大写，且与脚本中的 os.getenv 一致）。

在 Secret 框中粘贴你刚才复制的 Token 字符串。

点击 Add secret 保存。