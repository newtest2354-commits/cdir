import aiohttp
import asyncio
import logging
from collections import deque

SESSION = None

async def get_session():
    global SESSION
    if SESSION is None or SESSION.closed:
        SESSION = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=300,
                ssl=True
            )
        )
    return SESSION

async def close_session():
    global SESSION
    if SESSION and not SESSION.closed:
        await SESSION.close()
    SESSION = None

async def stream_urls(url, retries=3):
    session = await get_session()

    for attempt in range(retries):
        try:
            logging.debug(f"Fetching {url} (attempt {attempt + 1}/{retries})")
            async with session.get(url, timeout=40, ssl=True) as r:
                if r.status != 200:
                    logging.warning(f"HTTP {r.status} from {url}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    return

                buffer = bytearray()
                total_lines = 0

                async for chunk in r.content.iter_chunked(8192):
                    buffer.extend(chunk)

                    while b"\n" in buffer:
                        idx = buffer.index(b"\n")
                        line = buffer[:idx]
                        buffer = buffer[idx+1:]

                        line_str = line.decode(errors="ignore").strip()

                        if line_str:
                            yield line_str
                            total_lines += 1

                    if len(buffer) > 65536:
                        parts = buffer.split(b"\n")
                        buffer = parts[-1]

                if buffer:
                    line_str = buffer.decode(errors="ignore").strip()
                    if line_str:
                        yield line_str
                        total_lines += 1

                logging.debug(f"Fetched {total_lines} lines from {url}")
                return

        except asyncio.TimeoutError:
            logging.warning(f"Timeout fetching {url} (attempt {attempt + 1}/{retries})")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Error fetching {url}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                break

    logging.error(f"Failed to fetch {url} after {retries} attempts")
