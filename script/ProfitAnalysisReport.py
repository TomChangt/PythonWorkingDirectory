import os
import sys
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging
from datetime import datetime, timedelta
from EmailSender import EmailSender
from typing import List

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 获取文件名（不带扩展名）
current_file_name = os.path.splitext(os.path.basename(__file__))[0].upper()

# 加载环境变量
load_dotenv()


# 搜索数据库
def search_db(db_engine: create_engine, query: str) -> pd.DataFrame:
    return pd.read_sql(query, db_engine)


# 检查必要的环境变量
def check_required_env_vars() -> bool:
    required_vars = [
        "DB_USERNAME",
        "DB_PASSWORD",
        "DB_HOSTNAME",
        "SENDER_EMAIL",
        "EMAIL_PASSWORD",
        f"{current_file_name}_RECEIVER_EMAIL",
        f"{current_file_name}_CC_EMAIL",
    ]
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"缺少必要的环境变量: {var}")
            return False
    return True


# 获取上个月和当前月份的第一天
def get_last_and_current_month_first_day() -> tuple:
    today = datetime.now()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = last_day_of_previous_month.replace(day=1)

    return first_day_of_previous_month.strftime(
        "%Y-%m-%d"
    ), first_day_of_current_month.strftime("%Y-%m-%d")


# 执行查询 type 0 表示 渠道商，type 1 表示 终端
def execute_queries(
    db_engine: create_engine, last_month_first: str, current_month_first: str, type: int
) -> List[pd.DataFrame]:
    query_conditions = {
        0: {"order_sn": "%D%", "purchase_sn": "%C%"},  # 渠道商
        1: {"order_sn": "%G%", "purchase_sn": "%Z%"},  # 终端
    }

    if type not in query_conditions:
        raise ValueError("type 必须是 0 或 1")

    conditions = query_conditions[type]

    queries = [
        f"""
        SELECT 
            osc.name AS '一级品类', 
            ROUND(SUM(item.price * item.quantity), 2) AS '总价'
        FROM bo_order_item item
        LEFT JOIN bo_order o ON item.order_id = o.id
        LEFT JOIN bc_shop_goods_sku_rela gsr ON item.goods_id = gsr.goods_id
        LEFT JOIN bp_sku bs ON gsr.sku_id = bs.id
        LEFT JOIN bp_spu spu ON bs.spu_id = spu.id
        LEFT JOIN bp_spu_category_rela src ON spu.id = src.spu_id
        LEFT JOIN bp_spu_category sc ON sc.id = src.spu_category_id
        LEFT JOIN bp_spu_category osc ON osc.id = IF(sc.parent_id = 0, sc.id, SUBSTRING_INDEX(sc.path, ',', 1))
        WHERE 
            item.record_status = 1 
            AND src.record_status = 1 
            AND gsr.record_status = 1 
            AND item.p_order_item_id != 0
            AND o.deliver_time >= '{last_month_first}'
            AND o.deliver_time < '{current_month_first}'
            AND o.sn LIKE '{conditions['order_sn']}'
        GROUP BY osc.id
        """,
        f"""
        SELECT 
            osc.name AS '一级品类',
            ROUND(SUM(item.total_price), 2) AS '总价'
        FROM bo_order_after_sale_item item
        LEFT JOIN bo_order_after_sale s ON item.order_after_sale_id = s.id
        LEFT JOIN bo_order o ON s.order_id = o.id
        LEFT JOIN bc_shop_goods_sku_rela gsr ON item.goods_id = gsr.goods_id
        LEFT JOIN bp_sku bs ON gsr.sku_id = bs.id
        LEFT JOIN bp_spu spu ON bs.spu_id = spu.id
        LEFT JOIN bp_spu_category_rela src ON spu.id = src.spu_id
        LEFT JOIN bp_spu_category sc ON sc.id = src.spu_category_id
        LEFT JOIN bp_spu_category osc ON osc.id = IF(sc.parent_id = 0, sc.id, SUBSTRING_INDEX(sc.path, ',', 1))
        WHERE 
            item.record_status = 1 
            AND src.record_status = 1 
            AND gsr.record_status = 1
            AND s.after_sale_status = 8
            AND (
                (o.deliver_time >= '{last_month_first}' AND o.deliver_time < '{current_month_first}' AND s.modify_time < '{current_month_first}')
                OR (s.modify_time >= '{last_month_first}' AND s.modify_time < '{current_month_first}' AND o.deliver_time < '{last_month_first}')
            )
            AND o.sn LIKE '{conditions['order_sn']}'
        GROUP BY osc.id
        """,
        f"""
        SELECT 
            osc.name AS '一级品类', 
            ROUND(SUM(item.total_purchase_price), 2) AS '总价'
        FROM bo_purchase_item item
        LEFT JOIN bo_purchase p ON item.purchase_id = p.id
        LEFT JOIN bo_order o ON p.order_sn = o.sn
        LEFT JOIN bp_sku bs ON item.sku_id = bs.id
        LEFT JOIN bp_spu spu ON bs.spu_id = spu.id
        LEFT JOIN bp_spu_category_rela src ON spu.id = src.spu_id
        LEFT JOIN bp_spu_category sc ON sc.id = src.spu_category_id
        LEFT JOIN bp_spu_category osc ON osc.id = IF(sc.parent_id = 0, sc.id, SUBSTRING_INDEX(sc.path, ',', 1))
        LEFT JOIN (
            SELECT bspr.shipping_id, bspr.purchase_id, sh.start_time 
            FROM bl_shipping_purchase_rela bspr
            LEFT JOIN bl_shipping sh ON bspr.shipping_id = sh.id
            WHERE bspr.record_status = 1 AND sh.start_time IS NOT NULL 
            GROUP BY bspr.purchase_id
        ) ps ON p.id = ps.purchase_id
        WHERE 
            item.record_status = 1 
            AND src.record_status = 1 
            AND o.order_status IN (2,3,5)
            AND ps.start_time >= '{last_month_first}'
            AND ps.start_time < '{current_month_first}'
            AND p.sn LIKE '{conditions['purchase_sn']}'
        GROUP BY osc.id
        """,
        f"""
        SELECT 
            osc.name AS '一级品类', 
            ROUND(SUM(item.total_price), 2) AS '总价'
        FROM bo_purchase_after_sale_item item
        LEFT JOIN bo_purchase_after_sale pas ON item.purchase_after_sale_id = pas.id
        LEFT JOIN bo_purchase p ON item.purchase_id = p.id
        LEFT JOIN bo_order o ON p.order_sn = o.sn
        LEFT JOIN bp_sku bs ON item.sku_id = bs.id
        LEFT JOIN bp_spu spu ON bs.spu_id = spu.id
        LEFT JOIN bp_spu_category_rela src ON spu.id = src.spu_id
        LEFT JOIN bp_spu_category sc ON sc.id = src.spu_category_id
        LEFT JOIN bp_spu_category osc ON osc.id = IF(sc.parent_id = 0, sc.id, SUBSTRING_INDEX(sc.path, ',', 1))
        LEFT JOIN (
            SELECT bspr.shipping_id, bspr.purchase_id, sh.start_time 
            FROM bl_shipping_purchase_rela bspr
            LEFT JOIN bl_shipping sh ON bspr.shipping_id = sh.id
            WHERE bspr.record_status = 1 AND sh.start_time IS NOT NULL 
            GROUP BY bspr.purchase_id
        ) ps ON p.id = ps.purchase_id
        WHERE 
            item.record_status = 1 
            AND src.record_status = 1 
            AND pas.after_sale_status != 3 
            AND o.order_status IN (2,3,5)
            AND (
                (ps.start_time >= '{last_month_first}' AND ps.start_time < '{current_month_first}' AND pas.create_time < '{current_month_first}')
                OR (pas.create_time >= '{last_month_first}' AND pas.create_time < '{current_month_first}' AND ps.start_time < '{last_month_first}')
            )
            AND p.sn LIKE '{conditions['purchase_sn']}'
        GROUP BY osc.id
        """,
    ]

    return [search_db(db_engine, query) for query in queries]


def categorize(category: str) -> str:
    if category in ["刀具", "量具"]:
        return "刀具"
    elif category == "电气控制":
        return "电气控制"
    else:
        return "其他"


def process_query_results(df_list: List[pd.DataFrame]) -> pd.DataFrame:
    # 确保我们有4个DataFrame
    assert len(df_list) == 4, "需要4个查询结果"

    # 重命名列以反映数据的含义
    df_list[0] = df_list[0].rename(columns={"总价": "销售收入"})
    df_list[1] = df_list[1].rename(columns={"总价": "销售售后"})
    df_list[2] = df_list[2].rename(columns={"总价": "采购成本"})
    df_list[3] = df_list[3].rename(columns={"总价": "采购售后"})

    # 合并所有DataFrame
    merged_df = df_list[0].merge(df_list[1], on="一级品类", how="outer")
    merged_df = merged_df.merge(df_list[2], on="一级品类", how="outer")
    merged_df = merged_df.merge(df_list[3], on="一级品类", how="outer")

    # 填充NaN值为0
    merged_df = merged_df.fillna(0)

    # 应用分类规则
    merged_df["分类"] = merged_df["一级品类"].apply(categorize)

    # 按新分类进行分组并汇总
    grouped_df = (
        merged_df.groupby("分类")
        .agg(
            {"销售收入": "sum", "销售售后": "sum", "采购成本": "sum", "采购售后": "sum"}
        )
        .reset_index()
    )

    # 计算毛利和毛利率
    grouped_df["毛利"] = (
        grouped_df["销售收入"]
        - grouped_df["销售售后"]
        - (grouped_df["采购成本"] - grouped_df["采购售后"])
    )
    grouped_df["毛利率"] = grouped_df["毛利"] / (
        grouped_df["销售收入"] - grouped_df["销售售后"]
    )

    # 计算占比
    total_sales = grouped_df["销售收入"].sum()
    grouped_df["占比"] = grouped_df["销售收入"] / total_sales

    # 重新排序列
    grouped_df = grouped_df[
        [
            "分类",
            "销售收入",
            "销售售后",
            "采购成本",
            "采购售后",
            "毛利",
            "毛利率",
            "占比",
        ]
    ]

    # 添加合计行
    total_row = grouped_df.sum(numeric_only=True).to_frame().T
    total_row["分类"] = "合计"
    total_row["毛利率"] = total_row["毛利"] / (
        total_row["销售收入"] - total_row["销售售后"]
    )
    total_row["占比"] = 1  # 合计行的占比应该是100%

    final_df = pd.concat([grouped_df, total_row], ignore_index=True)

    # 格式化百分比列
    final_df["毛利率"] = final_df["毛利率"].apply(lambda x: f"{x:.2%}")
    final_df["占比"] = final_df["占比"].apply(lambda x: f"{x:.2%}")

    return final_df


def generate_excel(dataframes: List[pd.DataFrame], sheet_names: list, output_file: str):
    with pd.ExcelWriter(output_file) as writer:
        for df, sheet_name in zip(dataframes, sheet_names):
            df.to_excel(writer, sheet_name=sheet_name, index=False)


if __name__ == "__main__":
    # 在主函数开始处添加以下设置
    pd.set_option("display.float_format", lambda x: "%.2f" % x)

    if not check_required_env_vars():
        sys.exit(1)

    encoded_db_username = os.getenv("DB_USERNAME")
    encoded_db_password = os.getenv("DB_PASSWORD")
    db_hostname = os.getenv("DB_HOSTNAME")
    sender_email = os.getenv("SENDER_EMAIL")
    receiver_email = os.getenv(f"{current_file_name}_RECEIVER_EMAIL")
    cc_email = os.getenv(f"{current_file_name}_CC_EMAIL")
    password = os.getenv("EMAIL_PASSWORD")
    db_engine = create_engine(
        f"mysql+mysqlconnector://{quote_plus(encoded_db_username)}:{quote_plus(encoded_db_password)}@{db_hostname}/bwcmall"
    )
    try:
        last_month_first, current_month_first = get_last_and_current_month_first_day()

        # 渠道商
        trafficker_list = execute_queries(
            db_engine, last_month_first, current_month_first, 0
        )
        # 终端
        terminal_list = execute_queries(
            db_engine, last_month_first, current_month_first, 1
        )

        # 处理查询结果
        trafficker_df = process_query_results(trafficker_list)
        terminal_df = process_query_results(terminal_list)

        excel_file = "profit_analysis_report.xlsx"
        generate_excel(
            [trafficker_df, terminal_df], ["贸易商数据", "终端数据"], excel_file
        )

        # 发送邮件
        email_sender = EmailSender(sender_email, password)
        subject = "上月毛利分析报告"
        body = "请查看附件中的上月毛利分析报告。"
        email_sender.send_email(excel_file, receiver_email, cc_email, subject, body)
        # 删除文件
        os.remove(excel_file)
    except Exception as e:
        logger.error(f"发生错误: {e}")
    finally:
        db_engine.dispose()
