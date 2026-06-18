import asyncio
import time
import logging
from pipeline import producer, worker, OutputStore, AdaptiveConfig
from cache import Cache
from net import close_session as close_net_session
from stream import close_session as close_stream_session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

URLS = [
    "https://raw.githubusercontent.com/123jjck/cdn-ip-ranges/main/telegram/telegram_plain_ipv4.txt",
    "https://raw.githubusercontent.com/123jjck/cdn-ip-ranges/main/vercel/vercel_plain_ipv4.txt",
    "https://raw.githubusercontent.com/123jjck/cdn-ip-ranges/main/meta/meta_plain_ipv4.txt",
    "https://raw.githubusercontent.com/123jjck/cdn-ip-ranges/main/hetzner/hetzner_plain_ipv4.txt",
    "https://raw.githubusercontent.com/123jjck/cdn-ip-ranges/main/fastly/fastly_plain_ipv4.txt",
    "https://raw.githubusercontent.com/123jjck/cdn-ip-ranges/main/cloudflare/cloudflare_plain_ipv4.txt",
    "https://raw.githubusercontent.com/123jjck/cdn-ip-ranges/main/akamai/akamai_plain_ipv4.txt",
    "https://raw.githubusercontent.com/123jjck/cdn-ip-ranges/main/cdn77/cdn77_plain_ipv4.txt",
    "https://raw.githubusercontent.com/123jjck/cdn-ip-ranges/main/cogent/cogent_plain_ipv4.txt",
    "https://raw.githubusercontent.com/123jjck/cdn-ip-ranges/main/digitalocean/digitalocean_plain_ipv4.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/Afranet.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/AsiaTech-ip-CF.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/Iran%20Telecommunication%20Company%20PJS.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/Mizban%20Dade.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/MnageIt.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/Noyan%20Abr%20Arvan%20Co_1.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/Noyan%20Abr%20Arvan%20Co_2.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/Noyan%20Abr%20Arvan%20Co_3.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/Pars%20Abr.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/Pars%20Online.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/Respina.txt",
    "https://raw.githubusercontent.com/rezakhosh78/RKh-CF-Scanner/main/ip-ranges/iran/Tookan.txt"
]

WORKERS = 20
IPS_PER_URL = 10000
URL_DELAY = 3
TCP_CONCURRENCY = 30
TCP_BATCH_SIZE = 50
QUEUE_MAXSIZE = 8000
TCP_QUEUE_MAXSIZE = 500

async def run_cycle(cache):
    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    store = OutputStore("store.json")
    config = AdaptiveConfig()

    logging.info("Starting scan cycle")
    await producer(URLS, queue, cache, config, IPS_PER_URL, URL_DELAY, TCP_CONCURRENCY, TCP_BATCH_SIZE, TCP_QUEUE_MAXSIZE)

    workers = [asyncio.create_task(worker(queue, store, config)) for _ in range(WORKERS)]

    await queue.join()

    for _ in range(WORKERS):
        await queue.put(None)

    await asyncio.gather(*workers, return_exceptions=True)

    store.export_to_file("result.txt")
    logging.info(f"Scan cycle completed, found {len(store.latencies)} IPs")

    await close_net_session()
    await close_stream_session()

async def main():
    cache = Cache("cache.json")

    while True:
        start = time.time()

        await run_cycle(cache)
        await cache.flush()

        elapsed = time.time() - start
        await asyncio.sleep(max(0, 3600 - elapsed))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        pass
