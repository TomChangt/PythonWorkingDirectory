import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import numpy as np
import logging
import os
from dotenv import load_dotenv

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

start_time = "2024-01-01"
# URL 编码
encoded_db_username = quote_plus(os.getenv("DB_USERNAME"))
encoded_db_password = quote_plus(os.getenv("DB_PASSWORD"))
# 创建数据库引擎
kestrel_engine = create_engine(
    f"mysql+mysqlconnector://{encoded_db_username}:{encoded_db_password}@{os.getenv('DB_HOSTNAME')}/kestrel"
)
bwcmall_engine = create_engine(
    f"mysql+mysqlconnector://{encoded_db_username}:{encoded_db_password}@{os.getenv('DB_HOSTNAME')}/bwcmall"
)


def fetch_order_data(order_ids):
    order_ids_str = ", ".join(map(str, order_ids))
    ## 查询佰万仓订单
    order_sql = f"""
    select id as order_id, p_order_id as bwc_order_id, receivable 
    from bo_order 
    where record_status = 1 
    and p_order_id in ({order_ids_str})
    """
    df_order = pd.read_sql(order_sql, bwcmall_engine)
    o_ids_str = ", ".join(map(str, df_order["order_id"].tolist()))

    order_after_sql = f"""
    select order_id, sum(ifNull(amount, 0)) as sales_after_amount 
    from bo_order_after_sale 
    where record_status = 1
    and order_id in ({o_ids_str})
    and after_sale_status = 8
    GROUP BY order_id
    """
    df_order_after = pd.read_sql(order_after_sql, bwcmall_engine)
    rs = pd.merge(df_order, df_order_after, on="order_id", how="left")
    rs.fillna(0, inplace=True)
    rs["order_amount"] = rs["receivable"] - rs["sales_after_amount"]
    rs.drop(columns=["receivable", "sales_after_amount"], inplace=True)
    return rs


def fetch_purchase_data(order_ids):
    order_ids_str = ", ".join(map(str, order_ids))

    purchase_sql = f"""
    select p.id as purchase_id, p.payable as purchase_amount, r.order_id 
    from bo_purchase p 
    left join bo_order_purchase_rela r on p.id = r.purchase_id
    where p.record_status = 1 
    and r.record_status = 1 
    and p.purchase_status != 2
    and r.order_id in ({order_ids_str})       
    """
    df_purchase = pd.read_sql(purchase_sql, bwcmall_engine)
    purchase_ids_str = ", ".join(map(str, df_purchase["purchase_id"].tolist()))

    purchase_after_sql = f"""
    select purchase_id, SUM(IFNULL(amount, 0)) as purchase_after_amount 
    from bo_purchase_after_sale 
    where record_status = 1 
    and purchase_id in ({purchase_ids_str})  
    group by purchase_id 
    """
    df_purchase_after = pd.read_sql(purchase_after_sql, bwcmall_engine)
    rs = pd.merge(df_purchase, df_purchase_after, on="purchase_id", how="left")
    rs.fillna(0, inplace=True)
    rs["cost"] = rs["purchase_amount"] - rs["purchase_after_amount"]
    rs.drop(columns=["purchase_amount", "purchase_after_amount"], inplace=True)
    return rs


try:
    query_order = f"""
    select terminal_name, create_time, sn, bwc_order_id, order_status 
    from ko_order 
    where record_status = 1 
    and bwc_order_id is not null
    and order_status != 2 
    and create_time >= '{start_time}'
    ORDER BY create_time desc 
    """
    df_kestrel_order = pd.read_sql(query_order, kestrel_engine)
    logger.info(f"销售数据查询完成，查询到 {len(df_kestrel_order)} 条记录")
    p_order_ids = df_kestrel_order["bwc_order_id"].tolist()
    batch_size = 1000  # 每批次的大小，可以根据需要调整
    df_calculate_data = pd.DataFrame()

    for i in range(0, len(p_order_ids), batch_size):
        batch_order_ids = p_order_ids[i : i + batch_size]
        logger.info(f"查询第 {i // batch_size + 1} 批次的数据")
        df_order_batch = fetch_order_data(batch_order_ids)
        df_purchase_batch = fetch_purchase_data(df_order_batch["order_id"].tolist())
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

    df_calculate_data = pd.merge(
        df_kestrel_order, df_calculate_data, on="bwc_order_id", how="left"
    )
    df_calculate_data.fillna(0, inplace=True)

    df_calculate_data.rename(
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

    df_calculate_data["下单时间"] = df_calculate_data["下单时间"].dt.strftime(
        "%Y年%m月%d日"
    )

    conditions = [
        (df_calculate_data["订单状态"] == 0),
        (df_calculate_data["订单状态"] == 1),
        (df_calculate_data["订单状态"] == 2),
        (df_calculate_data["订单状态"] == 3),
        (df_calculate_data["订单状态"] == 4),
        (df_calculate_data["订单状态"] == 5),
    ]
    choices = ["待确认", "待发货", "已取消", "已发货", "已完成", "已完成"]
    df_calculate_data["订单状态"] = np.select(conditions, choices, default="待确认")

    columns_to_export = ["订单号", "用户名称", "下单时间", "订单状态", "总金额", "毛利"]

    # 将数据框写入 Excel 文件
    with pd.ExcelWriter("./data/order_export.xlsx") as writer:
        df_calculate_data.to_excel(
            writer, columns=columns_to_export, sheet_name="Orders", index=False
        )

    print("数据已成功导出到 order_export.xlsx")

except Exception as e:
    logger.error(f"发生错误: {e}")
finally:
    kestrel_engine.dispose()
    bwcmall_engine.dispose()
