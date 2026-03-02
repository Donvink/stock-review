import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import pytz
from google import genai

import warnings


# 忽略特定的 DeprecationWarning
# warnings.filterwarnings("ignore", category=UserWarning, module="py_mini_racer")
# 忽略所有警告
warnings.filterwarnings("ignore")


def load_local_csv(file_path=""):
    """从本地 CSV 文件加载数据"""
    if os.path.exists(file_path):
        # print(f"📂 发现本地缓存，正在读取: {file_path}")
        df = pd.read_csv(file_path, dtype={'代码': str}) # 强制代码列为字符串，防止 000001 变成 1
        return df
    else:
        # print(f"⚠️ 本地文件不存在: {file_path}")
        return None

def transfer_value(value):
    """将数值转换为亿元或万元的字符串表示"""
    if pd.isna(value):
        return value
    num = float(value)
    if num >= 1e8:
        return f"{num / 1e8:.2f} 亿"
    elif num >= 1e4:
        return f"{num / 1e4:.2f} 万"
    else:
        return f"{num:.2f}"

def stock_summary(date="20260213", save_dir='data'):
    """获取大盘数据"""
    file_path = f"{save_dir}/index_{date}.csv"

    # 1. 各大指数摘要数据
    index_df = load_local_csv(file_path)
    if index_df is not None:
        # total_amount = index_df.loc[len(index_df) - 1, '成交额']
        return index_df
    else:
        try:
            # index_df = ak.stock_zh_index_spot_em()
            index_df = ak.stock_zh_index_spot_sina()
            # print(index_df)
        except Exception as e:
            print(f"⚠️ 获取指数数据失败: {e}")
            return None

    target_indices = ["sh000001", "sz399001"]

    # 2. 筛选出两只指数
    result = index_df[index_df['代码'].isin(target_indices)].copy()

    # 3. 数据清理：将字符串转为数值
    result['成交额'] = pd.to_numeric(result['成交额'])
    result['涨跌幅'] = pd.to_numeric(result['涨跌幅'])

    # 4. 计算汇总成交额
    total_amount = result['成交额'].sum()

    # 5. 构造“汇总”行数据
    summary_row = {
        '代码': 'Total',
        '名称': '沪深总成交额',
        '最新价': None,  # 汇总行不需要最新价
        '成交额': total_amount,
        '涨跌幅': None  # 两个指数的涨幅不能直接相加，所以填 None 或保持为空
    }

    # 6. 将汇总行追加到 DataFrame 中
    # 使用 pd.DataFrame 转换一下再连接
    result = pd.concat([result, pd.DataFrame([summary_row])], ignore_index=True)

    # 7. 格式化输出：将成交额转为“亿元”更直观
    result['成交额(亿元)'] = result['成交额'].apply(transfer_value)
    result.insert(0, '序号', range(1, len(result) + 1))
    result.to_csv(file_path, index=False, encoding="utf-8-sig")
    
    print("-" * 30)
    print(result[['序号', '代码', '名称', '最新价', '涨跌幅', '成交额(亿元)']])
    print("-" * 30)
    return result

def reorder_columns(df, priority_cols):
    """将 DataFrame 的列按照优先级重新排序"""
    df = df.drop(columns=['序号', 'index', 'level_0'], errors='ignore')
    existing_priority = [c for c in priority_cols if c in df.columns]
    other_cols = [c for c in df.columns if c not in existing_priority]
    new_column_order = existing_priority + other_cols
    df_reorder = df[existing_priority + other_cols].copy()
    df_reorder.insert(0, '序号', range(1, len(df_reorder) + 1))
    return df_reorder

def rename_zt_cal_value(df):
    """将 '涨停统计' 和 '连板数' 列中的特定值替换为新值"""
    if '涨停统计' in df.columns:
        for index, row in df.iterrows():
            ori_value = row['涨停统计']
            cal_day = ori_value.split('/')[0] if isinstance(ori_value, str) else ori_value
            continue_day = ori_value.split('/')[1] if isinstance(ori_value, str) else ori_value
            if cal_day == '1':
                df.loc[index, '涨停统计'] = "首板"
            else:
                df.loc[index, '涨停统计'] = f"{cal_day}天{continue_day}板" if pd.notna(ori_value) else ori_value
    # if '连板数' in df.columns:
    #     df['连板数'] = df['连板数'].replace(1, '首板')
    return df

def stock_zt_dt_pool(date="20260213", save_dir='data'):
    """获取涨停/跌停个股数据"""
    zt_file_path = f"{save_dir}/zt_pool_{date}.csv"
    dt_file_path = f"{save_dir}/dt_pool_{date}.csv"
    zb_file_path = f"{save_dir}/zb_pool_{date}.csv"

    # 1. 各大指数摘要数据
    zt_pool_df = load_local_csv(zt_file_path)
    dt_pool_df = load_local_csv(dt_file_path)
    zb_pool_df = load_local_csv(zb_file_path)
    if zt_pool_df is None or dt_pool_df is None or zb_pool_df is None:
        try:
            zt_pool_df = ak.stock_zt_pool_em(date=date)
            time.sleep(0.5) # 避免请求过快被封
            dt_pool_df = ak.stock_zt_pool_dtgc_em(date=date)
            time.sleep(0.5) # 避免请求过快被封
            zb_pool_df = ak.stock_zt_pool_zbgc_em(date=date)
            time.sleep(0.5) # 避免请求过快被封

            zt_pool_df.sort_values(by='连板数', ascending=False, inplace=True)
            
            zt_pool_df['成交额'] = zt_pool_df['成交额'].apply(transfer_value)
            zt_pool_df['流通市值'] = zt_pool_df['流通市值'].apply(transfer_value)
            zt_pool_df['总市值'] = zt_pool_df['总市值'].apply(transfer_value)
            
            dt_pool_df['成交额'] = dt_pool_df['成交额'].apply(transfer_value)
            dt_pool_df['流通市值'] = dt_pool_df['流通市值'].apply(transfer_value)
            dt_pool_df['总市值'] = dt_pool_df['总市值'].apply(transfer_value)

            zb_pool_df['成交额'] = zb_pool_df['成交额'].apply(transfer_value)
            zb_pool_df['流通市值'] = zb_pool_df['流通市值'].apply(transfer_value)
            zb_pool_df['总市值'] = zb_pool_df['总市值'].apply(transfer_value)

            # '涨停统计' '连板数' 值重命名
            rename_zt_cal_value(zt_pool_df)
            rename_zt_cal_value(zb_pool_df)

            priority_cols = ['名称', '代码', '连板数', '涨停统计']
            # 重排zt_pool_df列
            zt_pool_df = reorder_columns(zt_pool_df, priority_cols)
            # 重排zb_pool_df列
            zb_pool_df = reorder_columns(zb_pool_df, priority_cols)

            zt_pool_df.to_csv(zt_file_path, index=False, encoding="utf-8-sig")
            # print(f"✅ 成功获取涨停板数据，保存至: {zt_file_path}")
            dt_pool_df.to_csv(dt_file_path, index=False, encoding="utf-8-sig")
            # print(f"✅ 成功获取跌停板数据，保存至: {dt_file_path}")
            zb_pool_df.to_csv(zb_file_path, index=False, encoding="utf-8-sig")
            # print(f"✅ 成功获取炸板数据，保存至: {zb_file_path}")
        except Exception as e:
            print(f"⚠️ 获取涨停板数据失败: {e}")
            return None, None, None
            
    zt_stocks = len(zt_pool_df)
    dt_stocks = len(dt_pool_df)
    zb_stocks = len(zb_pool_df)

    print("-" * 30)
    print(f"📊 {date} 涨停股数量: {zt_stocks}，跌停股数量: {dt_stocks}，炸板股数量: {zb_stocks}")
    print("-" * 30)
    
    return zt_pool_df, dt_pool_df, zb_pool_df

def fetch_all_stock_data(date='20260213', save_dir='data', max_retries=3):
    """尝试抓取所有股票数据，失败则重试"""
    file_path = f"{save_dir}/A_stock_{date}.csv"

    df = load_local_csv(file_path)
    if df is None:
        sucess = False
        for i in range(max_retries):
            try:
                print(f"尝试第 {i+1} 次抓取...")
                # 核心接口
                if i % 2 == 0:
                    # 首选：东方财富实时接口（数据最全，含代码、名称、涨跌幅、成交额等）
                    df = ak.stock_zh_a_spot_em()
                elif i % 2 == 1:
                    # 备选 1：新浪接口（在云服务器上极其稳定，虽数据字段略少，但基本行情都有）
                    print("⚠️ 尝试使用新浪稳健接口...")
                    df = ak.stock_zh_a_spot()
                
                if df is not None and not df.empty:
                    df.to_csv(file_path, index=False, encoding="utf-8-sig")
                    print("✅ 数据抓取成功！")
                    print(f"💾 数据已存至: {file_path}")
                    sucess = True
                    break
            except Exception as e:
                print(f"⚠️ 第 {i+1} 次抓取异常: {e}")
                time.sleep(5) # 等 5 秒再试
        if not sucess:
            print("❌ 所有重试均失败。")
            # exit(1)
            return None, None, None, None
    
    # 计算涨跌个数
    df['涨跌'] = df['涨跌幅'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    up_count = df[df['涨跌'] == 1].shape[0]
    down_count = df[df['涨跌'] == -1].shape[0]
    flat_count = df[df['涨跌'] == 0].shape[0]

    print("-" * 30)
    print(f"📈 上涨股数: {up_count}, 📉 下跌股数: {down_count}, 📊 持平股数: {flat_count}")
    print("-" * 30)

    return df, up_count, down_count, flat_count

def get_latest_date(max_try=20):
    """获取最新可用数据的日期"""
    today = datetime.now().strftime("%Y%m%d")
    if max_try <= 0:
        return today

    try:
        zt_pool_df = ak.stock_lhb_detail_daily_sina(date=today)
        return today
    except Exception:
        i = 1
        while i <= max_try:
            check_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                time.sleep(0.5) # 避免请求过快被封
                zt_pool_df = ak.stock_lhb_detail_daily_sina(date=check_date)
                break
            except Exception:
                i += 1
        if i <= max_try:
            print(f"✅ 最新可用数据日期: {check_date} (通过回溯 {i} 天找到)")
            return check_date
        print(f"⚠️ 回溯{max_try}天后仍未找到可用数据，无法确定最新日期。")
        return None

def get_stocks_info(df):
    """获取个股所属板块/概念信息"""
    industry_frequency = {} # 统计行业出现频次的字典
    # 确保 DataFrame 包含必要的列，如果缺失则添加空列
    for col in ['板块代码', '板块名称', '主营业务', '板块次数']:
        if col not in df.columns:
            df[col] = None
    for index, row in df.iterrows():
        code = row['代码']
        code = code[-6:] if len(code) > 6 else code # 确保代码是6位
        # 判断是否是科创板（688开头）或创业板（300开头），如果是则加上前缀
        if code.startswith('688'):
            code = 'SH' + code
        elif code.startswith('300'):
            code = 'SZ' + code
        else:
            code = 'SH' + code if code.startswith('6') else 'SZ' + code
        try:
            # info_df = ak.stock_individual_info_em(symbol=code)    # 东方财富
            info_df = ak.stock_individual_basic_info_xq(symbol=code) # 雪球
            # print(info_df)
            info_dict = info_df.set_index('item')['value'].to_dict()
            ind_code = info_dict.get('affiliate_industry').get('ind_code')
            ind_name = info_dict.get('affiliate_industry').get('ind_name')
            df.at[index, '板块代码'] = ind_code
            df.at[index, '板块名称'] = ind_name
            df.at[index, '主营业务'] = info_dict.get('main_operation_business')
            
            # 统计行业出现频次的字典，优先获取出现频次较高的板块信息
            if ind_code is not None:
                industry_frequency[ind_code] = industry_frequency.get(ind_code, 0) + 1
                df.at[index, '板块次数'] = industry_frequency[ind_code]
            time.sleep(0.5) # 避免请求过快被封
        except Exception as e:
            print(f"⚠️ 获取 {code} 板块信息失败: {e}")
    return True

def get_top_amount_stocks(df, top_n=20, date="20260213", save_dir='data'):
    """获取成交额前 N 的个股信息"""
    file_path = f"{save_dir}/top_amount_stocks_{date}.csv"
    top_stocks_df = load_local_csv(file_path)
    if top_stocks_df is not None:
        print("-" * 30)
        print(top_stocks_df)
        print('-' * 30)
        return top_stocks_df
    else:
        try:
            top_stocks_df = df.sort_values(by='成交额', ascending=False).head(top_n).copy()

            top_stocks_df.reset_index(drop=True, inplace=True)
            top_stocks_df['成交额(亿元)'] = top_stocks_df['成交额'].apply(transfer_value)
            # top_stocks_df['竞价涨幅(%)'] = ((top_stocks_df['今开'] - top_stocks_df['昨收']) / top_stocks_df['昨收'] * 100).round(2)
            # top_stocks_df['实体涨幅(%)'] = ((top_stocks_df['最新价'] - top_stocks_df['今开']) / top_stocks_df['今开'] * 100).round(2)

            top_stocks_df = top_stocks_df[['代码', '名称', '最新价', '涨跌幅', '成交额(亿元)']]

            get_stocks_info(top_stocks_df)
        except Exception as e:
            print(f"⚠️ 获取成交额前 N 的个股信息失败: {e}")
            return None
    
    top_stocks_df.insert(0, '序号', range(1, len(top_stocks_df) + 1))

    print("-" * 30)
    print(f"📈 成交量前 {top_n} 个股信息:")
    print(top_stocks_df)
    print('-' * 30)

    # 保存到文件
    top_stocks_df.to_csv(file_path, index=False, encoding="utf-8-sig")

    return top_stocks_df

def get_industry_summary(date="20260213", save_dir='data'):
    """获取行业板块信息"""
    file_path = f"{save_dir}/industry_summary_{date}.csv"

    # 1. 各大指数摘要数据
    industry_summary_df = load_local_csv(file_path)
    if industry_summary_df is None:
        try:
            industry_summary_df = ak.stock_board_industry_summary_ths()
            # print(industry_summary_df)
        except Exception as e:
            print(f"⚠️ 获取行业板块数据失败: {e}")
            return None

    # 取top 5 行业板块数据
    industry_summary_df = industry_summary_df.head(5).copy()

    industry_summary_df.to_csv(file_path, index=False, encoding="utf-8-sig")
    
    print("-" * 30)
    # print(industry_summary_df[['代码', '名称', '最新价', '涨跌幅', '成交额(亿元)']])
    # industry_summary_df = industry_summary_df[['板块名称', '板块代码', '涨跌幅', '上涨家数', '下跌家数', '领涨股票', '领涨股票-涨跌幅']]
    print(industry_summary_df)
    print("-" * 30)
    return industry_summary_df

def get_concept_summary(date="20260213", save_dir='data', top_n=5):
    """获取概念板块信息"""
    file_path = f"{save_dir}/concept_summary_{date}.csv"

    concept_summary_df = load_local_csv(file_path)
    if concept_summary_df is None:
        try:
            concept_summary_df = ak.stock_board_concept_name_em()
            concept_summary_df['总市值'] = concept_summary_df['总市值'].apply(transfer_value)
            # print(concept_summary_df)
        except Exception as e:
            print(f"⚠️ 获取概念板块数据失败: {e}")
            return None

    # 取top_n 板块数据
    concept_summary_df = concept_summary_df.head(top_n).copy()

    concept_summary_df.to_csv(file_path, index=False, encoding="utf-8-sig")
    
    print("-" * 30)
    # print(industry_summary_df[['代码', '名称', '最新价', '涨跌幅', '成交额(亿元)']])
    print(concept_summary_df)
    print("-" * 30)
    return concept_summary_df

def get_concept_cons(df, date="20260213", save_dir='data', top_n=15):
    """获取概念板块成分股信息"""
    if df is None or df.empty:
        print("⚠️ 概念板块数据为空，无法获取成分股信息。")
        return None, None

    all_concept_cons = [] # 用于存储所有概念板块成分股数据
    all_concept_cons_topn = [] # 用于存储所有概念板块成分股数据

    num_concepts = df.shape[0]
    for i in range(num_concepts):
        file_path = f"{save_dir}/concept_cons_{i}_{date}.csv"
        concept_cons_df = load_local_csv(file_path)
        if concept_cons_df is not None:
            all_concept_cons.append(concept_cons_df)
            concept_cons_df.sort_values(by='涨跌幅', ascending=False, inplace=True)
            concept_cons_df = concept_cons_df.head(top_n).copy()
            all_concept_cons_topn.append(concept_cons_df)

    if len(all_concept_cons) < num_concepts:
        all_concept_cons = []
        try:
            for index, row in df.iterrows():
                concept_cons_df = ak.stock_board_concept_cons_em(symbol=row['板块名称'])
                # 取前top_n个成分股数据
                concept_cons_df.sort_values(by='涨跌幅', ascending=False, inplace=True)
                concept_cons_df['成交额'] = concept_cons_df['成交额'].apply(transfer_value)
                concept_cons_df['所属板块'] = row['板块名称']
                all_concept_cons.append(concept_cons_df)
                concept_cons_df = concept_cons_df.head(top_n).copy()
                all_concept_cons_topn.append(concept_cons_df)
                # print(concept_cons_df)
                file_path = f"{save_dir}/concept_cons_{index}_{date}.csv"
                concept_cons_df.to_csv(file_path, index=False, encoding="utf-8-sig")
                time.sleep(0.5) # 避免请求过快被封
        except Exception as e:
            print(f"⚠️ 获取概念板块成分股数据失败: {e}")
            return None, None
    
    print("-" * 30)
    all_concept_cons_df = pd.concat(all_concept_cons_topn, ignore_index=True)
    print(all_concept_cons_df)
    print("-" * 30)
    return all_concept_cons, all_concept_cons_topn

def get_lhb_data(date="20260213", save_dir='data'):
    """获取龙虎榜数据"""
    file_path = f"{save_dir}/lhb_{date}.csv"

    lhb_df = load_local_csv(file_path)
    if lhb_df is None:
        try:
            # lhb_df_ori = ak.stock_lhb_detail_em(start_date=date, end_date=date)
            lhb_df_ori = ak.stock_lhb_detail_daily_sina(date=date)
            
            # 去掉名称带有“ST”的股票
            col_name = '名称' if '名称' in lhb_df_ori.columns else '股票名称'
            lhb_df = lhb_df_ori[~lhb_df_ori[col_name].str.contains('ST', case=False, na=False)].copy()
            # 股票去重
            lhb_df.drop_duplicates(subset=[col_name], inplace=True)
            lhb_df.drop(columns=['序号'], inplace=True, errors='ignore')
            lhb_df.reset_index(drop=True, inplace=True)
            lhb_df.insert(0, '序号', range(1, len(lhb_df) + 1))
            # print(lhb_df_ori)
            lhb_df.to_csv(file_path, index=False, encoding="utf-8-sig")
        except Exception as e:
            print(f"⚠️ 获取龙虎榜数据失败: {e}")
            return None
    
    print("-" * 30)
    print(lhb_df)
    print("-" * 30)
    return lhb_df

def get_watchlist(top_amount_stocks_df,
                    zt_pool_df,
                    zb_pool_df,
                    dt_pool_df,
                    lhb_df,
                    concept_cons,
                    date="20260213",
                    save_dir='data'
                ):
    """
    获取精确属性的重点个股信息
    watchlist1 (大额异动池): 成交额前二十，且在涨/跌/炸停板上、或者在龙虎榜上、或者在涨幅前五的行业板块里的个股
    watchlist2 (风口涨停池): 涨停/炸板，且在涨幅前五的行业板块里的个股
    """
    file_path1 = f"{save_dir}/watchlist1_{date}.csv"
    file_path2 = f"{save_dir}/watchlist2_{date}.csv"

    watchlist1_df = load_local_csv(file_path1)
    watchlist2_df = load_local_csv(file_path2)

    if watchlist1_df is not None and watchlist2_df is not None:
        print("-" * 30)
        print("Watchlist 1 (大额异动池):")
        print(watchlist1_df)
        print("Watchlist 2 (风口涨停池):")
        print(watchlist2_df)
        print("-" * 30)
        return watchlist1_df, watchlist2_df
    
    # --- 1. 建立前五板块的成员名称库 ---
    # 合并前五个板块的所有成分股，仅提取名称用于匹配
    top_5_member_names = set()
    if concept_cons is not None:
        for df in concept_cons[:5]:
            if not df.empty:
                name_col = '名称' if '名称' in df.columns else '股票名称'
                top_5_member_names.update(df[name_col].tolist())
    # --- 2. 准备其他异动池名称 ---
    zt_names = set(zt_pool_df['名称']) if not zt_pool_df.empty else set()
    zb_names = set(zb_pool_df['名称']) if not zb_pool_df.empty else set()
    dt_names = set(dt_pool_df['名称']) if not dt_pool_df.empty else set()
    
    lhb_col = '名称' if '名称' in lhb_df.columns else '股票名称'
    lhb_names = set(lhb_df[lhb_col]) if not lhb_df.empty else set()

    # --- 3. 构造 watchlist1 ---
    # 条件：在 top_amount_stocks_df 中，且满足 (涨/跌/炸/龙/前五板块成员) 任意一个
    w1_mask = (
        top_amount_stocks_df['名称'].isin(zt_names) |
        top_amount_stocks_df['名称'].isin(dt_names) |
        top_amount_stocks_df['名称'].isin(zb_names) |
        top_amount_stocks_df['名称'].isin(lhb_names) |
        top_amount_stocks_df['名称'].isin(top_5_member_names)
    )
    watchlist1_df = top_amount_stocks_df[w1_mask].copy()

    # --- 4. 构造 Watchlist 2 ---
    # 逻辑：将涨停池和炸板池合并，提取它们的属性
    
    # 统一字段名（防止 zt_pool 和 zb_pool 字段微差导致合并错位）
    # 增加一个标签列区分“状态”
    zt_temp = zt_pool_df.copy()
    if not zt_temp.empty:
        zt_temp['当前状态'] = '涨停'
    
    zb_temp = zb_pool_df.copy()
    if not zb_temp.empty:
        zb_temp['当前状态'] = '炸板'
    
    # 合并两个池子 
    combined_limit_df = pd.concat([zt_temp, zb_temp], ignore_index=True, sort=False)
    
    if not combined_limit_df.empty:
        # 筛选：属于前五板块成员的个股
        watchlist2_df = combined_limit_df[combined_limit_df['名称'].isin(top_5_member_names)].copy()
        
        # 排序：先看状态（涨停在前），再看连板数（越高越前）
        # 注意：炸板池可能没有“连板数”字段，需要填充 0 避免排序报错
        if '连板数' in watchlist2_df.columns:
            watchlist2_df['连板数'] = watchlist2_df['连板数'].fillna(0)
            watchlist2_df.sort_values(by=['当前状态', '连板数'], ascending=[False, False], inplace=True)
    else:
        watchlist2_df = pd.DataFrame()

    watchlist1_df.drop(columns=['序号'], inplace=True, errors='ignore')
    watchlist1_df.reset_index(drop=True, inplace=True)
    watchlist1_df.insert(0, '序号', range(1, len(watchlist1_df) + 1))

    watchlist2_df.drop(columns=['序号'], inplace=True, errors='ignore')
    watchlist2_df.reset_index(drop=True, inplace=True)
    watchlist2_df.insert(0, '序号', range(1, len(watchlist2_df) + 1))
    
    print("-" * 30)
    print("Watchlist 1 (大额异动池):")
    print(watchlist1_df)
    print("-" * 30)
    print("Watchlist 2 (风口涨停池):")
    print(watchlist2_df)
    print("-" * 30)

    # 保存 watchlist 到本地文件
    watchlist1_df.to_csv(file_path1, index=False, encoding="utf-8-sig")
    watchlist2_df.to_csv(file_path2, index=False, encoding="utf-8-sig")

    return watchlist1_df, watchlist2_df

def create_hugo_post(market_summary, ai_analysis, save_dir='content/posts'):
    """生成 Hugo 博客的 Markdown 内容"""
    # 确保目录存在
    os.makedirs(save_dir, exist_ok=True)
    
    # 1. 处理瑞士时区
    swiss_tz = pytz.timezone('Europe/Zurich')
    # 2. 将时间往前拨 10 分钟，确保 100% 判定为“已发布”
    safe_now = datetime.now(swiss_tz) - timedelta(minutes=10)
    
    # 生成文件名和 ISO 时间戳
    date_filename = safe_now.strftime("%Y-%m-%d")
    # 格式示例: 2026-02-12T20:15:00+01:00
    formatted_date = safe_now.strftime("%Y-%m-%dT%H:%M:%S%z")
    
    filename = f"{save_dir}/stock-analysis-{date_filename}.md"
    display_title = f"A股全市场复盘：{date_filename} 深度解析及AI洞察"

    content = f"""---
title: "{display_title}"
date: {formatted_date}
tags: ["每日复盘", "重点个股", "行业板块", "市场分析"]
categories: ["每日更新"]
showToc: true
draft: false
---

## 📈 A股市场概览

{market_summary}

---

## 🤖 AI 深度分析与洞察

{ai_analysis}

---
*注：
1. 数据来源：AKShare。
2. 本文由AI辅助生成，旨在提供市场洞察和数据分析，非投资建议。
3. 声明：投资有风险，入市需谨慎。本文内容仅供参考，不构成任何投资建议或推荐。请根据自身情况做出独立判断。*
"""
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"成功生成报告: {filename}")
    print(f"文章发布时间设为: {formatted_date}")

def create_content(
        index_df, up_count, down_count,
        zt_pool_df, dt_pool_df, zb_pool_df,
        top_amount_stocks_df,
        concept_summary_df, concept_cons_topn,
        lhb_df,
        watchlist1_df, watchlist2_df,
        date="20260213",
        save_dir='data'
    ):
    """生成市场汇总的 Markdown 内容"""
    
    file_path = f"{save_dir}/market_summary_{date}.md"

    content = f"""---
date: A股全市场复盘 {date} 
---


### 📊 市场核心快照
- **上证指数**: {index_df.iloc[0]['最新价']:.2f} ({index_df.iloc[0]['涨跌幅']:.2f}%)
- **全市场成交总额**: {index_df.iloc[2]['成交额(亿元)']}
- **涨跌比**: {up_count} / {down_count}
- **涨停/跌停/炸板数**: {len(zt_pool_df)} / {len(dt_pool_df)} / {len(zb_pool_df)}

---

### 🔍 成交额前二十个股

{top_amount_stocks_df.to_markdown(index=False)}

---

### 🏆 行业板块分析
- **前五概念板块**（按涨幅排序）

{concept_summary_df.to_markdown(index=False) if concept_summary_df is not None else "暂无概念板块数据"}

- **各板块板块涨幅靠前个股**（按涨幅排序）

- 板块1. {concept_cons_topn[0]['所属板块'].iloc[0] if concept_cons_topn is not None else "暂无数据"}

{concept_cons_topn[0].to_markdown(index=False) if concept_cons_topn is not None else "暂无数据"}

- 板块2. {concept_cons_topn[1]['所属板块'].iloc[0] if concept_cons_topn is not None else "暂无数据"}

{concept_cons_topn[1].to_markdown(index=False) if concept_cons_topn is not None else "暂无数据"}

- 板块3. {concept_cons_topn[2]['所属板块'].iloc[0] if concept_cons_topn is not None else "暂无数据"}

{concept_cons_topn[2].to_markdown(index=False) if concept_cons_topn is not None else "暂无数据"}

- 板块4. {concept_cons_topn[3]['所属板块'].iloc[0] if concept_cons_topn is not None else "暂无数据"}

{concept_cons_topn[3].to_markdown(index=False) if concept_cons_topn is not None else "暂无数据"}

- 板块5. {concept_cons_topn[4]['所属板块'].iloc[0] if concept_cons_topn is not None else "暂无数据"}

{concept_cons_topn[4].to_markdown(index=False) if concept_cons_topn is not None else "暂无数据"}

---

### 💥 涨停/炸板个股

- 涨停池

{zt_pool_df.to_markdown(index=False)}

- 炸板池

{zb_pool_df.to_markdown(index=False)}

---

### 🚀 龙虎榜

{lhb_df.to_markdown(index=False)}

---

### ⭐ 重点个股 Watchlist
- **大额异动池**（成交额前二十，且在涨/跌/炸/龙虎榜/前五板块成员中）

{watchlist1_df.to_markdown(index=False)}

- **风口涨停池**（涨停/炸板，且在前五板块成员中）

{watchlist2_df.to_markdown(index=False)}

---

"""
    
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"成功生成市场数据汇总报告: {file_path}")
    print('-' * 30)
    print(content)
    print('-' * 30)

    return content

def fetch_and_save(date='20260213', save_dir='data'):
    """主函数：获取数据并保存"""
    # 获取大盘数据并保存
    index_df = stock_summary(date=date, save_dir=save_dir)

    # 获取涨停数据并保存
    zt_pool_df, dt_pool_df, zb_pool_df = stock_zt_dt_pool(date=date, save_dir=save_dir)
    # TODO: 连板数据分析

    # 获取所有股票数据并保存
    all_stocks_df, up_count, down_count, flat_count = fetch_all_stock_data(date=date, save_dir=save_dir, max_retries=3)

    # 成交量前二十的个股名称、成交额、涨幅、以及所属板块或者概念
    top_amount_stocks_df = get_top_amount_stocks(all_stocks_df, top_n=20, date=date, save_dir=save_dir)

    # 涨幅前五板块中涨停个股、连板高度（几天几板、首板后涨幅）
    # # 同花顺-同花顺行业一览表
    # industry_summary_df = get_industry_summary(date=latest_date, save_dir=save_dir)
    
    # 东方财富-概念板块 实时行情数据
    concept_summary_df = get_concept_summary(date=date, save_dir=save_dir)

    # 概念板块成分股数据
    concept_cons, concept_cons_topn = get_concept_cons(concept_summary_df, date=date, save_dir=save_dir)

    # 龙虎榜
    lhb_df = get_lhb_data(date=date, save_dir=save_dir)

    # 重点个股信息
    watchlist1_df, watchlist2_df = get_watchlist(
                                                    top_amount_stocks_df,
                                                    zt_pool_df,
                                                    zb_pool_df,
                                                    dt_pool_df,
                                                    lhb_df,
                                                    concept_cons,
                                                    date=date,
                                                    save_dir=save_dir
                                                )

    # TODO: 热度榜

    # TODO: 获取资讯

    # TODO: 分析报告

    # 生成content以供AI分析和生成文章
    market_summary = create_content(
        index_df=index_df,
        zt_pool_df=zt_pool_df,
        dt_pool_df=dt_pool_df,
        zb_pool_df=zb_pool_df,
        up_count=up_count,
        down_count=down_count,
        top_amount_stocks_df=top_amount_stocks_df,
        concept_summary_df=concept_summary_df,
        concept_cons_topn=concept_cons_topn,
        lhb_df=lhb_df,
        watchlist1_df=watchlist1_df,
        watchlist2_df=watchlist2_df,
        date=latest_date,
        save_dir=save_dir
    )

    return market_summary

def analyze_market_with_ai(market_summary, date='20260213', save_dir='data'):
    prompt = f"""
        角色设定：你是一位拥有 20 年经验的 A 股资深策略分析师，擅长从成交量能、板块轮动和连板梯队中洞察市场情绪。

        任务描述：请基于下方提供的【当日复盘数据】，进行多维度复盘：

        1. 🚩 市场情绪诊断
        - 结合涨跌比、涨跌停对比、炸板率及全市场成交额，定义当前市场阶段（如：放量普涨、缩量整理、高位分歧、冰点重启等）。
        - 评价当前赚钱效应与亏钱效应的分布情况。

        2. 💰 核心主线与资金流向
        - 分析【成交额前二十】和【行业涨幅榜】，识别出目前资金主要锁定的“热点板块”和“大容错板块”。
        - 判断市场风格：是偏向“题材炒作”还是“权重护盘”？

        3. 🪜 连板梯度与空间博弈
        - 识别【涨停池】中的最高板（空间板）及其带动的属性。
        - 重点解读【炸板池】中的个股信号：是高位减速、还是分歧后的良性分歧？

        4. ⚡ 重点异动个股分析
        - 请从【重点个股 Watchlist】中挑选 2-3 只最具代表性的个股（如大成交涨停、高低位切换的典型），推测其背后的逻辑（资产注入、政策利好、超跌反弹还是技术突破）。

        5. 🧭 次日交易策略建议
        - 给出明日关注的观察点：哪些板块具备“反包”潜力？哪些高位品种需防范“补跌”？
        - 明确操作基调（如：积极参与、逢高止盈、或者多看少动）。
        - 给出重点个股的操作建议（如：建仓、继续持有、部分止盈、或者观望）。

        ---
        **📊 当日复盘数据内容如下**:
        {market_summary}

        要求：专业、客观、语言简练，避免模棱两可。输出格式使用 Markdown 标题和列表，增强可读性。
    """
    
    # 初始化 Gemini
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    MODEL_NAME = 'gemini-2.5-flash'

    client = genai.Client(api_key=GEMINI_API_KEY)
    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt
    )

    # save AI analysis result to file
    file_path = f"{save_dir}/ai_analysis_{date}.md"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(response.text)
    print(f"AI 分析结果已保存至: {file_path}")

    print("-" * 30)
    print("AI 分析结果:")
    print(response.text)
    print("-" * 30)

    return response.text

def prepare_date_and_directory(backtrack_days=20):
    """准备最新日期和数据目录"""
    latest_date = get_latest_date(backtrack_days)
    # latest_date = datetime.now().strftime("%Y%m%d")
    if latest_date is None:
        print("❌ 无法确定最新数据日期，脚本终止。")
        exit(1)
    os.makedirs("data", exist_ok=True)
    save_dir = f"data/{latest_date}"
    os.makedirs(save_dir, exist_ok=True)

    return latest_date, save_dir

if __name__ == "__main__":
    latest_date, save_dir = prepare_date_and_directory(0)
    market_summary = fetch_and_save(date=latest_date, save_dir=save_dir)
    print("市场数据汇总已生成，正在进行AI分析...")
    ai_analysis = analyze_market_with_ai(market_summary, date=latest_date, save_dir=save_dir)
    print("AI分析完成，正在生成Hugo博客内容...")
    create_hugo_post(market_summary, ai_analysis, save_dir='content/posts')
                                                                                                                    