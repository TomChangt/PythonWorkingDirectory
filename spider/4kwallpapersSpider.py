import aiohttp
import asyncio
import aiofiles
from bs4 import BeautifulSoup
import logging
from typing import List, Dict
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

MAIN_URL = "https://4kwallpapers.com"
BASE_DOWNLOAD = Path("/Users/changtong/Downloads/4kwallpapers")
MAX_CONCURRENT_REQUESTS = 10
TIMEOUT = 60  # 设置60秒超时
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


async def fetch_url(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(
            url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=TIMEOUT)
        ) as response:
            if response.status == 200:
                return await response.text()
            else:
                logger.error(f"Failed to fetch {url}. Status: {response.status}")
                return ""
    except asyncio.TimeoutError:
        logger.error(f"Timeout error while fetching {url}")
        return ""
    except Exception as e:
        logger.error(f"Error fetching {url}: {str(e)}")
        return ""


async def parse_page(session: aiohttp.ClientSession, url: str) -> List[Dict[str, str]]:
    html = await fetch_url(session, url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = soup.find("div", id="pics-list").find_all("p", class_="wallpapers__item")

    results = []
    for item in items:
        title = item.find("a")["title"]
        pic_html_url = item.find("a")["href"]

        pic_html = await fetch_url(session, pic_html_url)
        if pic_html:
            psoup = BeautifulSoup(pic_html, "html.parser")
            pic_url = (
                MAIN_URL
                + psoup.find("span", class_="res-ttl").find_all("a")[-1]["href"]
            )
            results.append({"title": title, "url": pic_url})

    return results


async def download_pic(session: aiohttp.ClientSession, url: Dict[str, str]) -> None:
    try:
        async with session.get(
            url["url"], headers=HEADERS, timeout=aiohttp.ClientTimeout(total=TIMEOUT)
        ) as response:
            if response.status == 200:
                file_path = BASE_DOWNLOAD / f"{url['title']}.jpg"
                async with aiofiles.open(file_path, mode="wb") as f:
                    await f.write(await response.read())
            else:
                logger.error(
                    f"Failed to download {url['url']}. Status: {response.status}"
                )
    except asyncio.TimeoutError:
        logger.error(f"Timeout error while downloading {url['url']}")
    except Exception as e:
        logger.error(f"Error downloading {url['url']}: {str(e)}")


async def process_page(
    session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore
) -> None:
    async with semaphore:
        wallpapers = await parse_page(session, url)
        download_tasks = [download_pic(session, wallpaper) for wallpaper in wallpapers]
        await asyncio.gather(*download_tasks)


async def main() -> None:
    BASE_DOWNLOAD.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
        tasks = [
            process_page(
                session, f"{MAIN_URL}/?page={p}" if p != 1 else MAIN_URL, semaphore
            )
            for p in range(700, 731)
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
