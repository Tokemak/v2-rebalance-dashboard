import asyncio
import pandas as pd
import requests
import time
import random
from mainnet_launch.constants import ChainData, ETH_CHAIN, ALCHEMY_API_KEY

_rate_limiter = asyncio.Semaphore(200)


def get_nearest_block_before_timestamp_sync(timestamp: int, chain: ChainData) -> tuple[int, int | None]:
    # not reliable
    url = f"https://api.g.alchemy.com/data/v1/{ALCHEMY_API_KEY}/utility/blocks/by-timestamp"
    params = {
        "networks": chain.alchemy_network_enum,
        "timestamp": pd.to_datetime(int(timestamp), unit="s", utc=True).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "direction": "BEFORE",
    }
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers, params=params)
    attempts = 0
    while (response.status_code != 200) and attempts < 3:
        time.sleep((2**attempts) + random.random() / 10)
        attempts += 1

        response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:

        number = response.json()["data"][0]["block"]["number"]
    else:
        number = None
    return timestamp, number


async def get_nearest_block_by_timestamp_async(timestamp: int, chain: ChainData) -> tuple[int, int | None]:
    async with _rate_limiter:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, get_nearest_block_before_timestamp_sync, timestamp, chain)
        return result


async def get_nearest_blocks_for_timestamps_async(timestamps: list[int], chain: ChainData) -> dict:
    tasks = [get_nearest_block_by_timestamp_async(ts, chain) for ts in timestamps]
    results = await asyncio.gather(*tasks)
    return {timestamp: number for timestamp, number in results}


def get_nearest_blocks_for_timestamps(timestamps: list[int], chain: ChainData) -> dict:
    timestamp_to_number_dict = {}
    pending_timestamps = list(timestamps)
    while pending_timestamps:
        # Run only for pending timestamps using a single event loop for each batch
        result = asyncio.run(get_nearest_blocks_for_timestamps_async(pending_timestamps, chain))
        for ts, number in result.items():
            if number is not None:
                timestamp_to_number_dict[ts] = number
            print(len(timestamp_to_number_dict))

        pending_timestamps = [ts for ts in timestamps if ts not in timestamp_to_number_dict]
        print(f"timestamps found={len(timestamps) - len(pending_timestamps)}, pending={len(pending_timestamps)}")
        time.sleep(1)
    return timestamp_to_number_dict


# https://subgraph.satsuma-prod.com/community/blocks-base/playground

# {
#   blocks(
#     where: { number_in: [20000000, 20000002] }
#   ) {
#     id
#     number
#     timestamp
#     # You can include additional fields here if needed
#   }
# }


# idea


# load all the solver rebalance events
