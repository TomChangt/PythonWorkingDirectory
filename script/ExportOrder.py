import pandas as pd
import logging
import os
import sys
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv
from typing import List
from EmailSender import EmailSender

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 获取文件名（不带扩展名）
current_file_name = os.path.splitext(os.path.basename(__file__))[0].upper()

# 加载环境变量
load_dotenv()


def fetch_data(engine, sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, engine)


def fetch_order_data(order_ids: List[int], engine) -> pd.DataFrame:
    order_ids_str = ", ".join(map(str, order_ids))
    order_sql = f"""
    SELECT id as order_id, p_order_id as bwc_order_id, receivable 
    FROM bo_order 
    WHERE record_status = 1 AND p_order_id IN ({order_ids_str})
    """
    df_order = fetch_data(engine, order_sql)

    o_ids_str = ", ".join(map(str, df_order["order_id"].tolist()))
    order_after_sql = f"""
    SELECT order_id, SUM(IFNULL(amount, 0)) as sales_after_amount 
    FROM bo_order_after_sale 
    WHERE record_status = 1 AND order_id IN ({o_ids_str}) AND after_sale_status = 8
    GROUP BY order_id
    """
    df_order_after = fetch_data(engine, order_after_sql)

    rs = pd.merge(df_order, df_order_after, on="order_id", how="left").fillna(0)
    rs["order_amount"] = rs["receivable"] - rs["sales_after_amount"]
    return rs.drop(columns=["receivable", "sales_after_amount"])


def fetch_purchase_data(order_ids: List[int], engine) -> pd.DataFrame:
    order_ids_str = ", ".join(map(str, order_ids))
    purchase_sql = f"""
    SELECT p.id as purchase_id, p.payable as purchase_amount, r.order_id 
    FROM bo_purchase p 
    LEFT JOIN bo_order_purchase_rela r ON p.id = r.purchase_id
    WHERE p.record_status = 1 AND r.record_status = 1 AND p.purchase_status != 2 AND r.order_id IN ({order_ids_str})       
    """
    df_purchase = fetch_data(engine, purchase_sql)

    purchase_ids_str = ", ".join(map(str, df_purchase["purchase_id"].tolist()))
    purchase_after_sql = f"""
    SELECT purchase_id, SUM(IFNULL(amount, 0)) as purchase_after_amount 
    FROM bo_purchase_after_sale 
    WHERE record_status = 1 AND purchase_id IN ({purchase_ids_str})  
    GROUP BY purchase_id 
    """
    df_purchase_after = fetch_data(engine, purchase_after_sql)

    rs = pd.merge(df_purchase, df_purchase_after, on="purchase_id", how="left").fillna(
        0
    )
    rs["cost"] = rs["purchase_amount"] - rs["purchase_after_amount"]
    return rs.drop(columns=["purchase_amount", "purchase_after_amount"])


def process_data(df_kestrel_order: pd.DataFrame, bwcmall_engine) -> pd.DataFrame:
    p_order_ids = df_kestrel_order["bwc_order_id"].tolist()
    batch_size = 1000
    df_calculate_data = pd.DataFrame()

    for i in range(0, len(p_order_ids), batch_size):
        batch_order_ids = p_order_ids[i : i + batch_size]
        logger.info(f"查询第 {i // batch_size + 1} 批次的数据")
        df_order_batch = fetch_order_data(batch_order_ids, bwcmall_engine)
        df_purchase_batch = fetch_purchase_data(
            df_order_batch["order_id"].tolist(), bwcmall_engine
        )

        df_purchase_summary = (
            df_purchase_batch.groupby("order_id").agg({"cost": "sum"}).reset_index()
        )
        df_result = pd.merge(
            df_order_batch, df_purchase_summary, on="order_id", how="left"
        )
        df_result = (
            df_result.groupby("bwc_order_id")
            .agg({"order_amount": "sum", "cost": "sum"})
            .reset_index()
        )
        df_result["profit"] = df_result["order_amount"] - df_result["cost"]
        df_calculate_data = pd.concat([df_calculate_data, df_result], ignore_index=True)

    return pd.merge(
        df_kestrel_order, df_calculate_data, on="bwc_order_id", how="left"
    ).fillna(0)


def format_data(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(
        columns={
            "sn": "订单号",
            "terminal_name": "用户名称",
            "create_time": "下单时间",
            "order_status": "订单状态",
            "order_amount": "总金额",
            "profit": "毛利",
        },
        inplace=True,
    )
    df["下单时间"] = df["下单时间"].dt.strftime("%Y年%m月%d日")
    status_map = {
        0: "待确认",
        1: "待发货",
        2: "已取消",
        3: "已发货",
        4: "已完成",
        5: "已完成",
    }
    df["订单状态"] = df["订单状态"].map(status_map).fillna("待确认")
    return df


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


if __name__ == "__main__":

    if not check_required_env_vars():
        sys.exit(1)

    # 从环境变量获取配置
    encoded_db_username = os.getenv("DB_USERNAME")
    encoded_db_password = os.getenv("DB_PASSWORD")
    db_hostname = os.getenv("DB_HOSTNAME")
    sender_email = os.getenv("SENDER_EMAIL")
    receiver_email = os.getenv(f"{current_file_name}_RECEIVER_EMAIL")
    cc_email = os.getenv(f"{current_file_name}_CC_EMAIL")
    password = os.getenv("EMAIL_PASSWORD")

    # 创建数据库引擎
    kestrel_engine = create_engine(
        f"mysql+mysqlconnector://{quote_plus(encoded_db_username)}:{quote_plus(encoded_db_password)}@{db_hostname}/kestrel"
    )
    bwcmall_engine = create_engine(
        f"mysql+mysqlconnector://{quote_plus(encoded_db_username)}:{quote_plus(encoded_db_password)}@{db_hostname}/bwcmall"
    )
    try:
        query_order = """
        SELECT terminal_name, create_time, sn, bwc_order_id, order_status 
        FROM ko_order 
        WHERE record_status = 1 AND bwc_order_id IS NOT NULL AND order_status != 2 AND create_time >= '2024-01-01'
        ORDER BY create_time DESC 
        """
        df_kestrel_order = fetch_data(kestrel_engine, query_order)
        logger.info(f"销售数据查询完成，查询到 {len(df_kestrel_order)} 条记录")

        df_calculate_data = process_data(df_kestrel_order, bwcmall_engine)
        df_formatted = format_data(df_calculate_data)

        # 在导出之前将 bwc_order_id 转换为字符串类型
        df_formatted["bwc_order_id"] = df_formatted["bwc_order_id"].astype(str)

        file_name = "order_export.xlsx"
        df_formatted.to_excel(file_name, index=False)

        email_body = "请查收附件中的订单报表。"
        email_sender = EmailSender(sender_email, password)
        email_sender.send_email(
            file_name, receiver_email, cc_email, "订单报表", email_body
        )
        os.remove(file_name)
        logger.info(f"Excel文件 {file_name} 已删除")

    except Exception as e:
        logger.error(f"发生错误: {e}")
    finally:
        kestrel_engine.dispose()
        bwcmall_engine.dispose()
