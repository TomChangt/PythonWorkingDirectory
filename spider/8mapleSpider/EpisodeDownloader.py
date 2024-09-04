import aiohttp
import aiofiles
import asyncio
import logging
import re
import json
import os
import shutil
from typing import Optional, List

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class EpisodeDownloader:
    def __init__(self, url: str, headers: dict):
        self.url = url
        self.headers = headers

    async def download_with_retry(
        self,
        session: aiohttp.ClientSession,
        url: str,
        retries: int = 10,
        delay: int = 5,
    ) -> Optional[bytes]:
        for attempt in range(retries):
            try:
                async with session.get(url, headers=self.headers) as res:
                    res.raise_for_status()
                    return await res.read()
            except aiohttp.ClientError as e:
                logger.error(f"下载失败: {e}, 尝试重试 {attempt + 1}/{retries}")
                await asyncio.sleep(delay)
        logger.error(f"下载失败: {url} 在重试 {retries} 次后仍然失败")
        return None

    async def fetch_url(
        self, session: aiohttp.ClientSession, url: str
    ) -> Optional[str]:
        try:
            async with session.get(url, headers=self.headers) as res:
                res.raise_for_status()
                return await res.text()
        except aiohttp.ClientError as e:
            logger.error(f"请求失败: {e}")
            return None

    def extract_player_data(self, html_content: str) -> Optional[dict]:
        player_pattern = (
            r"<script type=\"text/javascript\">var player_aaaa=(.*?)</script>"
        )
        player_match = re.search(player_pattern, html_content, re.DOTALL)
        if player_match:
            try:
                return json.loads(player_match.group(1).strip())
            except json.JSONDecodeError:
                logger.error("解析player_aaaa数据失败")
        else:
            logger.error("未找到player_aaaa数据")
        return None

    async def fetch_ts_files(
        self, session: aiohttp.ClientSession, m3u8_url: str
    ) -> List[str]:
        base_path = "/".join(m3u8_url.split("/")[:-1])
        m3u8_content = await self.fetch_url(session, m3u8_url)
        if m3u8_content:
            mixed_pattern = r"(?m)^.*$"
            mixed_match = re.findall(mixed_pattern, m3u8_content)
            if mixed_match:
                mixed_url = f"{base_path}/{mixed_match[-1]}"
                mixed_content = await self.fetch_url(session, mixed_url)
                if mixed_content:
                    rs = re.findall(r"\b\w+\.ts\b", mixed_content)
                    base_path = "/".join(mixed_url.split("/")[:-1])
                    return [f"{base_path}/{ts_file}" for ts_file in rs]
                else:
                    logger.error(f"请求mixed.m3u8失败，url = {mixed_url}")
            else:
                logger.error("未能匹配到 mixed_match")
        else:
            logger.error(f"请求index.m3u8失败，url = {m3u8_url}")
        return []

    async def download_ts_files(
        self, session: aiohttp.ClientSession, ts_files: List[str], video_dir: str
    ) -> List[str]:
        tasks = []
        for ts_file in ts_files:
            tasks.append(self.download_and_save(session, ts_file, video_dir))
        return await asyncio.gather(*tasks)

    async def download_and_save(
        self, session: aiohttp.ClientSession, ts_file: str, video_dir: str
    ) -> str:
        content = await self.download_with_retry(session, ts_file)
        if content:
            ts_path = os.path.join(video_dir, os.path.basename(ts_file))
            async with aiofiles.open(ts_path, "wb") as f:
                await f.write(content)
            return ts_path
        else:
            logger.error(f"跳过文件: {ts_file}")
            return ""

    def merge_ts_files(self, ts_files: List[str], output_path: str) -> None:
        with open(output_path, "wb") as output_file:
            for ts_file in ts_files:
                if ts_file:
                    with open(ts_file, "rb") as f:
                        output_file.write(f.read())
        logger.info(f"合并完成: {output_path}")

    async def download_episode(self, session: aiohttp.ClientSession) -> None:
        html_content = await self.fetch_url(session, self.url)
        if html_content:
            player_data = self.extract_player_data(html_content)
            title = re.findall(r"<title>(.*?)</title>", html_content)[0]
            title = "-".join(title.split("|")[:2])
            logger.info(f"开始下载: {title}")
            if player_data:
                m3u8_url = player_data.get("url")
                if m3u8_url:
                    ts_files = await self.fetch_ts_files(session, m3u8_url)
                    video_dir = os.path.join("./8mapleSpider/爱回家", title)
                    os.makedirs(video_dir, exist_ok=True)
                    logger.info(f"创建临时文件夹: {video_dir}")
                    downloaded_ts_files = await self.download_ts_files(
                        session, ts_files, video_dir
                    )
                    video_path = os.path.join("./8mapleSpider/爱回家", f"{title}.mp4")
                    self.merge_ts_files(downloaded_ts_files, video_path)
                    shutil.rmtree(video_dir)
                    logger.info(f"删除临时文件夹: {video_dir}")
                    logger.info(f"下载完成: {video_path}")
                else:
                    logger.error("未找到 m3u8 URL")
            else:
                logger.error("未找到 player 数据")
        else:
            logger.error("未能获取 HTML 内容")
