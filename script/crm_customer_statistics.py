import asyncio
import logging
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text
import pandas as pd
from urllib.parse import quote_plus
from motor.motor_asyncio import AsyncIOMotorClient

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
# 加载环境变量
load_dotenv()


async def fetch_data(db_engine: AsyncEngine, query: str) -> pd.DataFrame:
    async with db_engine.connect() as conn:
        result = await conn.execute(text(query))
        df = pd.DataFrame(result.fetchall())
        return df


async def fetch_last_operators(db: AsyncIOMotorClient, customer_ids: list) -> dict:
    try:
        # 将所有customer_id转为整数
        customer_ids = [int(cid) for cid in customer_ids]

        # 使用聚合管道，一次性获取所有客户的最后操作记录
        pipeline = [
            {"$match": {"customerId": {"$in": customer_ids}}},
            {"$sort": {"operationTime": -1}},
            {
                "$group": {
                    "_id": "$customerId",
                    "operatorName": {"$first": "$operatorName"},
                }
            },
        ]

        results = {}
        async for doc in db.crm_customer_log.aggregate(pipeline):
            results[str(doc["_id"])] = doc["operatorName"]

        # 对于没有找到记录的客户，填充默认值
        return {str(cid): results.get(str(cid), "-") for cid in customer_ids}

    except Exception as e:
        logger.error(f"Error fetching last operators: {e}")
        return {str(cid): "-" for cid in customer_ids}


async def main():
    engine = None
    mongo_client = None
    try:
        # 从环境变量获取 MongoDB 连接信息
        mongo_uri = os.getenv("MONGO_URI")
        encoded_db_username = os.getenv("DB_USERNAME")
        encoded_db_password = os.getenv("DB_PASSWORD")
        db_hostname = os.getenv("DB_HOSTNAME")
        engine = create_async_engine(
            f"mysql+aiomysql://{quote_plus(encoded_db_username)}:{quote_plus(encoded_db_password)}@{db_hostname}/crm"
        )
        # 创建 MongoDB 客户端时添加认证源
        mongo_client = AsyncIOMotorClient(
            mongo_uri,
            authSource="bwcmall",  # 指定认证数据库
            serverSelectionTimeoutMS=5000,  # 设置超时时间
        )
        db = mongo_client.bwcmall
        # 客户id转成字符串
        query = """
            select CAST(c.id AS CHAR) as '客户id', c.name as '公司名称',c.contacts as '联系人',c.phone as '手机号码', 
            case c.is_resource WHEN 1 THEN '资源' ELSE '客户' end as '类型',
            ss.value as '销售进程',cl.`value` as '客户等级',
            ifnull((
                    SELECT GROUP_CONCAT(cTT.value SEPARATOR ',')
                    FROM crm_customer_tag c_tag 
                LEFT JOIN  crm_customer_dict cTT on (cTT.`code` = 'customer_tag' and c_tag.tag_id = cTT.id) 
                    WHERE c_tag.customer_id = c.id and c_tag.record_status = 1
                ) ,'-')AS '客户标签',
            ifnull(ce.business_scope,'-') as '生产产品' ,
            ifnull(cco.`value`,'-') as '设备数量',
            ifnull(DATE_FORMAT(c.follow_up_time,'%Y-%m-%d'),'-') as '最后跟进时间',
            ifnull(DATE_FORMAT((
                select crt.modify_time from crm_customer_return crt where crt.customer_id = c.id
                ORDER BY crt.modify_time desc limit 1
            ),'%Y-%m-%d'),'-') as '退回公海时间',
            ifnull((
                select crtn.remark from crm_customer_return crtn where crtn.customer_id = c.id
                ORDER BY crtn.modify_time desc limit 1
            ),'-') as '退回原因',
            ifnull(u.username,'-') as '最后跟进人'
            from crm_customer c 
            left join crm_customer_dict ss on (ss.`code` = 'customer_process' and c.process = ss.id) 
            left join crm_customer_dict cl on (cl.`code` = 'customer_level' and c.`level` = cl.id) 
            left join crm_customer_extend ce on c.id = ce.customer_id
            left join crm_customer_dict cco on (cco.`code` = 'purchase_quantity' and ce.purchase_quantity = cco.id)
            left join crm_user u on c.follow_up_user = u.id
            where c.`owner` is NULL and c.record_status = 1 and c.source = 10000001 
            and EXISTS(
            select 1 from crm_customer_return cr where cr.create_time > '2023-05-01' and cr.customer_id = c.id)
        """
        df = await fetch_data(engine, query)

        # 批量获取所有客户的最后操作人信息
        customer_ids = df["客户id"].tolist()
        operators_dict = await fetch_last_operators(db, customer_ids)

        # 使用映射更新DataFrame
        df["最后操作人"] = df["客户id"].map(operators_dict)

        file_name = "customer_statistics.xlsx"
        df.to_excel(file_name, index=False)
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
    finally:
        if engine:
            await engine.dispose()
        if mongo_client:
            mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
