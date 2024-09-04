import os
import sys
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging
from datetime import datetime, timedelta
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


def search_db(db_engine: create_engine, query: str) -> pd.DataFrame:
    return pd.read_sql(query, db_engine)


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


def generate_excel(dataframes: list, sheet_names: list, output_file: str):
    with pd.ExcelWriter(output_file) as writer:
        for df, sheet_name in zip(dataframes, sheet_names):
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def get_last_month() -> str:
    """
    获取上个月的月份，格式为'YYYY-MM'
    """
    last_month = datetime.now().replace(day=1) - timedelta(days=1)
    return last_month.strftime("%Y-%m")


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
    db_engine = create_engine(
        f"mysql+mysqlconnector://{quote_plus(encoded_db_username)}:{quote_plus(encoded_db_password)}@{db_hostname}/crm"
    )
    try:
        last_month = get_last_month()
        queries = [
            f"""
                select
                user.username as '顾问',
                if(u_count is not null, u_count, 0) as '下单客户数',
                if(o_count is not null, o_count, 0) as '订单数量',
                if(o_amount is not null, o_amount, 0) as '订单总金额',
                if(a_amount is not null, a_amount, 0)  as '售后总金额'
                from crm_user user
                left join
                (
                select owner, count(*) as 'u_count', sum(count) as 'o_count', sum(amount) as 'o_amount' from 
                (
                    select crm_customer.owner, crm_customer.id, crm_customer.platform_account, crm_customer.name, sum(o.receivable) as amount, count(*) as count from crm_customer
                    left join 
                    (
                        select p_order_id as id, sum(receivable) as receivable, customer_id from bwcmall.bo_order 
                        where record_status=1 and order_status not in (0, 4, 6) and left(payment_date,7)='{last_month}' group by p_order_id
                    ) as o on o.customer_id=crm_customer.platform_account
                    where crm_customer.record_status and crm_customer.owner is not null 
                        and crm_customer.platform_account is not null
                        and o.id is not null
                    group by crm_customer.owner, crm_customer.id order by crm_customer.owner
                ) as o group by o.owner
                ) as odata on user.id=odata.owner
                left join
                (
                select owner, sum(amount) as 'a_amount' from
                (
                    select crm_customer.owner, crm_customer.id, crm_customer.platform_account, crm_customer.name, sum(o.amount) as amount, count(*) as count from crm_customer
                    left join bwcmall.bo_order_after_sale o on o.record_status=1 and o.order_sn LIKE'%G%' and o.after_sale_status=8 and o.customer_id=crm_customer.platform_account
                    where crm_customer.record_status and crm_customer.owner is not null
                        and crm_customer.platform_account is not null
                        and o.id is not null and left(o.modify_time,7)='{last_month}'
                    group by crm_customer.owner, crm_customer.id order by crm_customer.owner
                ) as a  group by a.owner
                ) as oafter on user.id=oafter.owner
                where u_count > 0
            """,
            f"""
                select 
                user.username as '顾问',
                odata.name as '客户名称',
                odata.count as '订单数量',
                odata.amount as '订单总金额'
                from
                (
                select crm_customer.owner, crm_customer.id, crm_customer.platform_account, crm_customer.name, sum(o.receivable) as amount, count(*) as count from crm_customer
                left join
                (
                select p_order_id as id, sum(receivable) as receivable, customer_id from bwcmall.bo_order 
                where record_status=1 and order_status not in (0, 4, 6) and left(payment_date,7)='{last_month}' group by p_order_id
                ) as o on o.customer_id=crm_customer.platform_account
                where crm_customer.record_status and crm_customer.owner is not null 
                and crm_customer.platform_account is not null
                and o.id is not null
                group by crm_customer.owner, crm_customer.id order by crm_customer.owner
                ) as odata
                left join crm_user user on user.id=odata.owner
            """,
            f"""
                select
                user.username as '顾问', 
                oafter.name as '客户名称',
                oafter.count as '售后单数',
                oafter.amount as '售后总金额'
                from
                (
                select crm_customer.owner, crm_customer.id, crm_customer.platform_account, crm_customer.name, sum(o.amount) as amount, count(*) as count from crm_customer
                left join bwcmall.bo_order_after_sale o on o.record_status=1 and o.after_sale_status=8 and o.customer_id=crm_customer.platform_account
                where crm_customer.record_status and crm_customer.owner is not null 
                    and crm_customer.platform_account is not null
                    and o.id is not null and left(o.modify_time,7)='{last_month}'
                group by crm_customer.owner, crm_customer.id order by crm_customer.owner
                ) as oafter 
                left join crm_user user on user.id=oafter.owner
            """,
        ]
        sheet_names = ["SQL（总计）", "订单明细", "售后明细"]
        dataframes = []

        for query in queries:
            df = search_db(db_engine, query)
            dataframes.append(df)

        output_file = f"consultant_trade_report_{last_month}.xlsx"
        generate_excel(dataframes, sheet_names, output_file)

        # 发送邮件
        email_sender = EmailSender(sender_email, password)
        subject = f"顾问交易报告 - {last_month}"
        body = f"请查看附件中的{last_month}月顾问交易报告。"
        email_sender.send_email(output_file, receiver_email, cc_email, subject, body)

        # 删除文件
        os.remove(output_file)
    except Exception as e:
        logger.error(f"发生错误: {e}")
    finally:
        db_engine.dispose()
