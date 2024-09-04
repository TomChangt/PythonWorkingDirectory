import os
import sys
import pandas as pd
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging
from datetime import datetime, timedelta
from email.mime.text import MIMEText

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()


def search_db(db_engine: create_engine, query: str) -> pd.DataFrame:
    return pd.read_sql(query, db_engine)


def generate_excel(df: pd.DataFrame) -> str:
    # 生成Excel文件
    excel_file = "result.xlsx"
    df.to_excel(excel_file, index=False)
    return excel_file


def get_previous_two_months(date: datetime = None):
    if date is None:
        date = datetime.now()
    current_month = date.replace(day=1)
    last_month = (current_month - timedelta(days=1)).replace(day=1)
    two_months_ago = (last_month - timedelta(days=1)).replace(day=1)
    return (
        two_months_ago.strftime("%Y年%m月") + "总金额",
        last_month.strftime("%Y年%m月") + "总金额",
        two_months_ago.strftime("%Y-%m-%d"),
        last_month.strftime("%Y-%m-%d"),
        current_month.strftime("%Y-%m-%d"),
    )


def send_email(
    file_path: str,
    sender_email: str,
    receiver_email: str,
    cc_email: str,
    password: str,
    subject: str,
    smtp_server: str = "smtp.exmail.qq.com",
    smtp_port: int = 465,
) -> None:

    # 创建邮件
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Cc"] = cc_email
    message["Subject"] = subject

    # 添加邮件正文
    body = "请查收附件中的前两个月新客首单统计。"
    message.attach(MIMEText(body, "plain"))
    # 添加附件
    with open(file_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename= {file_path}")
    message.attach(part)

    # 发送邮件
    with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
        server.login(sender_email, password)
        server.send_message(message, to_addrs=[receiver_email] + cc_email.split(","))


def check_required_env_vars():
    required_vars = [
        "DB_USERNAME",
        "DB_PASSWORD",
        "DB_HOSTNAME",
        "SENDER_EMAIL",
        "RECEIVER_EMAIL",
        "EMAIL_PASSWORD",
    ]
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"缺少必要的环境变量: {var}")
            sys.exit(1)


if __name__ == "__main__":

    check_required_env_vars()

    # 从环境变量获取配置
    encoded_db_username = os.getenv("DB_USERNAME")
    encoded_db_password = os.getenv("DB_PASSWORD")
    db_hostname = os.getenv("DB_HOSTNAME")
    sender_email = os.getenv("SENDER_EMAIL")
    receiver_email = os.getenv("RECEIVER_EMAIL")
    cc_email = os.getenv("CC_EMAIL", "")  # 提供默认值
    password = os.getenv("EMAIL_PASSWORD")

    # 创建数据库引擎
    kestrel_engine = create_engine(
        f"mysql+mysqlconnector://{quote_plus(encoded_db_username)}:{quote_plus(encoded_db_password)}@{db_hostname}/kestrel"
    )
    bwcmall_engine = create_engine(
        f"mysql+mysqlconnector://{quote_plus(encoded_db_username)}:{quote_plus(encoded_db_password)}@{db_hostname}/bwcmall"
    )
    try:
        (
            two_months_ago_desc,
            last_month_desc,
            two_months_ago,
            last_month,
            current_month,
        ) = get_previous_two_months()
        subject = f"【{two_months_ago} 至 {current_month}】新客首单统计"
        query_order = f"""
            select t.crop_name as crop_name,case t.customer_type when '1' then '终端' when '2' then '贸易商' end as customer_type,
            temp.first_order as order_time,temp.first_order_id as bwc_order_id
            ,(select IFNULL(sum(receivable),0) from ko_order where record_status = 1
            and order_type = 2 and order_status not in (0,2)
            and user_terminal_info_id = temp.user_terminal_info_id
            and create_time > '{two_months_ago}' and create_time <= '{last_month}') as two_months_ago_amount
            ,(select IFNULL(sum(receivable),0) from ko_order where record_status = 1
            and order_type = 2 and order_status not in (0,2)
            and user_terminal_info_id = temp.user_terminal_info_id
            and create_time > '{last_month}' and create_time <= '{current_month}') as last_month_amount
            from (
            select user_terminal_info_id,create_time as first_order,bwc_order_id as first_order_id from ko_order where record_status = 1
            and order_type = 2 and order_status not in (0,2)
            group by user_terminal_info_id) temp
            left join kc_user_terminal_info t on temp.user_terminal_info_id = t.id
            where temp.first_order > '{two_months_ago}' and temp.first_order <= '{current_month}'
        """
        order_data_df = search_db(kestrel_engine, query_order)
        o_ids_str = ", ".join(map(str, order_data_df["bwc_order_id"].tolist()))
        query_advisor = f"""
            select o.id as bwc_order_id,c.first_name as advisor_name from bo_order o
            left join bs_crm_user_rela cur on o.customer_id = cur.customer_id
            left join bc_customer c on cur.crm_user_id = c.user_id
            where o.record_status = 1  and c.record_status = 1
            and o.id in ({o_ids_str})
        """
        advisor_data_df = search_db(bwcmall_engine, query_advisor)
        df_result = pd.merge(
            order_data_df, advisor_data_df, on="bwc_order_id", how="left"
        )
        # 移除某一列
        df_result.drop(columns=["bwc_order_id"], inplace=True)
        df_result.rename(
            columns={
                "crop_name": "公司名称",
                "customer_type": "客户类型",
                "order_time": "首单时间",
                "two_months_ago_amount": two_months_ago_desc,
                "last_month_amount": last_month_desc,
                "advisor_name": "顾问名称",
            },
            inplace=True,
        )
        df_result["首单时间"] = df_result["首单时间"].dt.strftime("%Y年%m月%d日")
        excel_file = generate_excel(df_result)
        send_email(
            excel_file, sender_email, receiver_email, cc_email, password, subject
        )
        # 删除文件
        os.remove(excel_file)
    except Exception as e:
        logger.error(f"发生错误: {str(e)}")
    finally:
        kestrel_engine.dispose()
        bwcmall_engine.dispose()
