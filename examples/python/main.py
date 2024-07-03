#!/usr/bin/env python3

from __future__ import annotations
import asyncio
import time
import json

import aiohttp


def main() -> None:
    """An example of how we can send records asynchronously to Seb from Python."""
    t0 = time.time()

    payload_num = 500
    asyncio.run(
        add_records_async(
            async_limit=500,
            base_url="http://127.0.0.1:51313",
            topic_name="haps",
            records=[
                json.dumps({"payload": i}).encode("utf8") for i in range(payload_num)
            ],
        )
    )

    elapsed = time.time() - t0
    print(f"added {payload_num} in {elapsed}")


async def add_records_async(
    async_limit: int, base_url: str, topic_name: str, records: list[bytes]
) -> None:
    # the number of open connections limits how many records we can add to the
    # same record batch.
    connector = aiohttp.TCPConnector(limit=async_limit)

    async with aiohttp.ClientSession(base_url=base_url, connector=connector) as session:
        client = Client(session)
        add_calls = [
            client.add_record(topic_name=topic_name, data=payload)
            for payload in records
        ]

        await asyncio.gather(*add_calls)


class Client:
    _session: aiohttp.ClientSession
    _base_url: str

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    async def add_record(self, topic_name: str, data: bytes) -> None:
        url = f"/add?topic-name={topic_name}"
        async with self._session.post(url, data=data) as response:
            print(response.status)


if __name__ == "__main__":
    main()
