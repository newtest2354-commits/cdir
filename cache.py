import json
import os
import time
import asyncio
import logging

TTL = 48 * 3600

class Cache:
    def __init__(self, path):
        self.path = path
        self.data = {}
        self.lock = asyncio.Lock()
        self.last_cleanup = time.time()
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r") as f:
                self.data = json.load(f)
            logging.info(f"Loaded cache with {len(self.data)} entries")
        except:
            self.data = {}
            logging.warning("Failed to load cache, starting fresh")
        now = time.time()
        old_count = len(self.data)
        self.data = {
            k: v for k, v in self.data.items()
            if now - v < TTL
        }
        if len(self.data) < old_count:
            logging.info(f"Removed {old_count - len(self.data)} expired cache entries")
        self.last_cleanup = now

    def _cleanup(self):
        now = time.time()
        if len(self.data) < 200000:
            return
        sorted_items = sorted(self.data.items(), key=lambda x: x[1])
        remove_count = len(sorted_items) // 5
        for k, _ in sorted_items[:remove_count]:
            self.data.pop(k, None)
        if len(self.data) < len(sorted_items):
            logging.debug(f"Cleaned {remove_count} oldest cache entries")
        self.last_cleanup = now

    async def check_and_claim(self, ip):
        async with self.lock:
            now = time.time()
            if len(self.data) > 200000:
                self._cleanup()
            ts = self.data.get(ip)
            if ts is not None and now - ts < TTL:
                return False
            self.data[ip] = now
            return True

    async def mark_success(self, ip):
        async with self.lock:
            self.data[ip] = time.time()

    async def flush(self):
        async with self.lock:
            self._cleanup()
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.data, f, separators=(",", ":"))
            os.replace(tmp, self.path)
            logging.info(f"Cache flushed with {len(self.data)} entries")
