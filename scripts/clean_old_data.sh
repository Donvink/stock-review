#!/bin/bash
# 清理 data/ 目录下超过 60 天的子目录
# 目录名格式：YYYYMMDD

DATA_DIR="data"
# 计算 60 天前的日期（格式 YYYYMMDD）
CUTOFF_DATE=$(date -d "60 days ago" +%Y%m%d)

for dir in "$DATA_DIR"/*/; do
    # 提取目录名（去掉路径和末尾斜杠）
    dirname=$(basename "$dir")
    # 检查目录名是否为 8 位数字（简单校验）
    if [[ $dirname =~ ^[0-9]{8}$ ]]; then
        if [[ $dirname -lt $CUTOFF_DATE ]]; then
            echo "Removing old directory: $dir"
            rm -rf "$dir"
        fi
    fi
done