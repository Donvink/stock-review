#!/bin/bash
# 每月打包 data/ 目录下所有文件并上传到 GitHub Releases
# 触发条件：通常在每月1日执行

DATA_DIR="data"
# 获取当前年月（例如 2026-03）
YEAR_MONTH=$(date +%Y-%m)
ARCHIVE_NAME="data-${YEAR_MONTH}.tar.gz"

# 打包整个 data 目录
tar -czf "$ARCHIVE_NAME" "$DATA_DIR"

# 使用 gh 命令行上传到 Releases
# 注意：需要确保已安装 gh 并配置好 GITHUB_TOKEN
TAG="data-${YEAR_MONTH}"
TITLE="Monthly data archive ${YEAR_MONTH}"

# 创建 Release（如果已存在则忽略错误，继续上传附件）
gh release create "$TAG" --title "$TITLE" --notes "Monthly data backup for ${YEAR_MONTH}" || true
# 上传附件（如果已存在则覆盖）
gh release upload "$TAG" "$ARCHIVE_NAME" --clobber

# 可选：删除本地压缩包
rm "$ARCHIVE_NAME"