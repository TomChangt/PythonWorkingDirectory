import asyncio
import logging
import random
import re
import aiohttp
from bs4 import BeautifulSoup
from EpisodeDownloader import EpisodeDownloader

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 来几个不同的 headers 随机取一个User-Agent
HEADERS_LIST = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36"
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36"
    },
]
MAX_CONCURRENT_REQUESTS = 3
DOMAIN = "https://8maple.pm"


async def fetch_page(url: str, session: aiohttp.ClientSession) -> str | None:
    async with session.get(url, headers=random.choice(HEADERS_LIST)) as response:
        if response.status != 200:
            logger.error(f"Failed to fetch {url}. Status: {response.status}")
            return None
        return await response.text()


async def download_episode_with_semaphore(
    link: str, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore
):
    async with semaphore:
        await EpisodeDownloader(link, random.choice(HEADERS_LIST)).download_episode(
            session
        )


async def main():
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        url = "https://8maple.pm/play/gg1C-1-662.html"
        page_content = await fetch_page(url, session)
        if page_content:
            soup = BeautifulSoup(page_content, "html.parser")
            playlist = soup.find("ul", id="playlist")
            if not playlist:
                logger.error("Playlist not found")
                return
            items = playlist.find_all("li")
            links = [
                f'{DOMAIN}{item.find("a")["href"]}'
                for item in items
                if item.find("a")
                and 2442
                < int(re.search(r"第(.*?)集", item.find("a").text).group(1))
                <= 2447
            ]
            tasks = [
                download_episode_with_semaphore(link, session, semaphore)
                for link in links
            ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
