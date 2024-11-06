import asyncio
import logging
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy import text
from typing import List, Optional
import pandas as pd
from pydantic import BaseModel

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_SERVER = os.getenv("POSTGRES_SERVER")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}/{POSTGRES_DB}"

ENGINE = create_async_engine(
    DB_URL,
    echo=True,
    poolclass=AsyncAdaptedQueuePool,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30,
    pool_recycle=1800,
)

AsyncSessionLocal = async_sessionmaker(
    ENGINE,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


class CategoryPath(BaseModel):
    id: int
    name: str
    path: str


class ExcelData(BaseModel):
    category: str
    logo: str


async def read_excel_data(file_path: str) -> List[ExcelData]:
    df = pd.read_excel(file_path)
    return [
        ExcelData(category=row["分类"], logo=row["图片"]) for _, row in df.iterrows()
    ]


async def get_category_paths(session: AsyncSession) -> List[CategoryPath]:
    query = text(
        """
    WITH RECURSIVE category_tree AS (
        SELECT id, name->>'en' as name, CAST(name->>'en' as TEXT) as path
        FROM category
        WHERE parent_id IS NULL
        
        UNION ALL
        
        SELECT c.id, c.name->>'en', 
               ct.path || ' > ' || (c.name->>'en')
        FROM category c
        JOIN category_tree ct ON c.parent_id = ct.id
    )
    SELECT id, name, path FROM category_tree
    ORDER BY path;
    """
    )

    result = await session.execute(query)
    return [CategoryPath(id=row.id, name=row.name, path=row.path) for row in result]


async def update_category_logo(
    session: AsyncSession, category_id: int, logo_url: str
) -> None:
    query = text("UPDATE categories SET logo = :logo WHERE id = :id")
    await session.execute(query, {"logo": logo_url, "id": category_id})


async def find_best_match(
    excel_path: str, db_paths: List[CategoryPath]
) -> Optional[int]:
    excel_parts = excel_path.split(" > ")
    best_match: Optional[int] = None
    max_match_length = 0

    for db_path in db_paths:
        db_parts = db_path.path.split(" > ")
        match_length = sum(
            1 for a, b in zip(reversed(excel_parts), reversed(db_parts)) if a == b
        )
        if match_length > max_match_length:
            max_match_length = match_length
            best_match = db_path.id

    return best_match


async def process_data(data: List[ExcelData]) -> None:
    async with AsyncSessionLocal() as session:
        try:
            async with session.begin():
                category_paths = await get_category_paths(session)

                for item in data:
                    best_match_id = await find_best_match(item.category, category_paths)

                    if best_match_id:
                        ##await update_category_logo(session, best_match_id, item.logo)
                        print(
                            f"Updated category: {item.category} with logo: {item.logo}"
                        )
                    else:
                        print(f"No match found for: {item.category}")
        except Exception as e:
            print(f"An error occurred: {e}")


async def main() -> None:
    excel_file_path = "/Users/changtong/Downloads/cat-all.xlsx"
    data = await read_excel_data(excel_file_path)
    await process_data(data)


if __name__ == "__main__":

    asyncio.run(main())
