import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os
import logging
import aiohttp
import asyncio
from dotenv import load_dotenv

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
base_directory = "/Users/changtong/Downloads/SpuSku图片导出"

# 加载环境变量
load_dotenv()


def read_excel(file_path: str, sheet_name: str, start_row: int = 1) -> pd.DataFrame:
    return pd.read_excel(file_path, sheet_name=sheet_name, skiprows=start_row)


def search_db(db_engine: create_engine, query: str) -> pd.DataFrame:
    return pd.read_sql(query, db_engine)


async def download_image(
    session: aiohttp.ClientSession, resource: str, spu_directory: str
) -> None:
    image_name = f"{os.path.basename(resource)}"
    image_path = os.path.join(spu_directory, image_name)

    async with session.get(resource) as response:
        if response.status == 200:
            with open(image_path, "wb") as file:
                file.write(await response.read())
            logger.info(f"Downloaded {resource} ....")
        else:
            logger.info(f"Failed to download {image_name} from {resource}")


async def download_images_for_group(
    group: pd.DataFrame,
    df_spu_pic_res: pd.DataFrame = None,
    df_sku_pic_res: pd.DataFrame.groupby = None,
) -> None:
    # 创建spu文件夹
    spu_name = group["spu_name"].iloc[0]
    spu_directory = os.path.join(base_directory, spu_name)
    os.makedirs(spu_directory, exist_ok=True)
    tasks = []
    async with aiohttp.ClientSession() as session:
        if df_spu_pic_res is not None:
            for resource in df_spu_pic_res["resources"]:
                task = download_image(session, resource, spu_directory)
                tasks.append(task)
        if df_sku_pic_res is not None:
            df_sku_pic_filtered = group[group["sku_id"].isin(df_sku_pic_res.groups)][
                ["sku_id", "sku_name"]
            ]
            for sku_data in df_sku_pic_filtered.itertuples(index=False):
                sku_dir = os.path.join(spu_directory, sku_data.sku_name)
                os.makedirs(sku_dir, exist_ok=True)
                for r in df_sku_pic_res.get_group(sku_data.sku_id)["resources"]:
                    task = download_image(session, r, sku_dir)
                    tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks)


def main() -> None:
    # URL 编码
    encoded_db_username = quote_plus(os.getenv("DB_USERNAME"))
    encoded_db_password = quote_plus(os.getenv("DB_PASSWORD"))
    # 创建数据库引擎
    db_engine = create_engine(
        f"mysql+mysqlconnector://{encoded_db_username}:{encoded_db_password}@{os.getenv('DB_HOSTNAME')}/bwcmall"
    )
    try:
        df = read_excel("./data/delData.xlsx", "商品信息", 4)
        # 获取第一列数据
        goods_ids = df.iloc[:, 0].drop_duplicates().tolist()
        goods_ids_str = ", ".join(map(str, goods_ids))

        # 获取商品对应的sku和spu信息
        get_goods_sku_spu_sql = f"""
        select g.id as goods_id ,g.name as goods_name , s.id as sku_id,s.name as sku_name,spu.id as spu_id,spu.name as spu_name from bc_shop_goods g 
        left join bc_shop_goods_sku_rela gsr on g.id = gsr.goods_id 
        left join bp_sku s on gsr.sku_id = s.id
        left join bp_spu spu on s.spu_id = spu.id
        where g.id in ({goods_ids_str}) 
        and g.record_status = 1
        and gsr.record_status = 1
        and s.record_status = 1
        and spu.record_status = 1 
        """
        df_goods_sku_spu = search_db(db_engine, get_goods_sku_spu_sql)

        # 获取spu对应的图片信息
        spu_ids = df_goods_sku_spu["spu_id"].drop_duplicates().tolist()
        spu_ids_str = ", ".join(map(str, spu_ids))
        get_spu_pic_sql = f"""
        select sr.spu_id,r.resources from bp_spu_res_rela sr 
        left join bs_resources r on sr.resources_id = r.id 
        where sr.record_status = 1 and r.record_status = 1
        and sr.spu_id in ({spu_ids_str}) 
        """
        df_spu_pic = search_db(db_engine, get_spu_pic_sql)
        df_spu_pic_grouped = df_spu_pic.groupby("spu_id")

        # 获取sku对应的图片信息
        sku_ids = df_goods_sku_spu["sku_id"].drop_duplicates().tolist()
        sku_ids_str = ", ".join(map(str, sku_ids))
        get_sku_res_sql = f"""
        select sr.sku_id,r.resources from bp_sku_res_rela sr 
        left join bs_resources r on sr.resources_id = r.id 
        where sr.record_status = 1 and r.record_status = 1
        and sr.sku_id in ({sku_ids_str}) 
        """
        df_sku_pic = search_db(db_engine, get_sku_res_sql)

        for spu_id, group in df_goods_sku_spu.groupby("spu_id"):
            df_filtered_sku_pic = df_sku_pic[
                df_sku_pic["sku_id"].isin(group["sku_id"])
            ].groupby("sku_id")
            if spu_id in df_spu_pic_grouped.groups:
                asyncio.run(
                    download_images_for_group(
                        group, df_spu_pic_grouped.get_group(spu_id), df_filtered_sku_pic
                    )
                )
            else:
                asyncio.run(
                    download_images_for_group(group, df_sku_pic_res=df_filtered_sku_pic)
                )

    except Exception as e:
        logger.error(f"发生错误: {e}")
    finally:
        db_engine.dispose()


if __name__ == "__main__":
    main()
