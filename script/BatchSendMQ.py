import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import logging
import os
import pika
from pika.exceptions import AMQPConnectionError
from dotenv import load_dotenv

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()


def search_db(db_engine: create_engine, query: str) -> pd.DataFrame:
    return pd.read_sql(query, db_engine)


def send_messages(
    parameters: pika.ConnectionParameters,
    queue_name: str,
    routing_key: str,
    messages: list,
) -> None:
    try:
        with pika.BlockingConnection(parameters) as connection:
            with connection.channel() as channel:
                channel.queue_declare(queue=queue_name, durable=True)
                for message in messages:
                    channel.basic_publish(
                        exchange="elastic.job.exchange.topic",
                        routing_key=routing_key,
                        body=str(message),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # 使消息持久化
                        ),
                        mandatory=True,
                    )
                    print(f" Send msg : '{message}'")
    except AMQPConnectionError as error:
        print(f"无法连接到RabbitMQ服务器: {error}")
    except Exception as error:
        print(f"发生错误: {error}")


def main() -> None:
    # URL 编码
    encoded_db_username = quote_plus(os.getenv("DB_USERNAME"))
    encoded_db_password = quote_plus(os.getenv("DB_PASSWORD"))
    # 创建数据库引擎
    db_engine = create_engine(
        f"mysql+mysqlconnector://{encoded_db_username}:{encoded_db_password}@{os.getenv('DB_HOSTNAME')}/bwcecatalog"
    )
    sql = "select id from ebp_service_buyer_product where modify_time > '2024-07-23'"
    df = search_db(db_engine, sql)["id"].drop_duplicates().tolist()

    # RabbitMQ 服务器连接参数
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USERNAME"), os.getenv("RABBITMQ_PASSWORD")
    )
    parameters = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST"),  # 远程主机名或IP地址
        port=5672,  # 默认端口，如果不同请修改
        virtual_host="/",  # 默认虚拟主机，如果不同请修改
        credentials=credentials,
    )
    send_messages(
        parameters,
        "elastic.job.queue.syncEproductByProductId",
        "elastic.job.routing.key.syncEproductByProductId",
        df,
    )


if __name__ == "__main__":
    main()
