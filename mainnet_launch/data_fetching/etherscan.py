# too slow

# import asyncio
# import requests
# from concurrent.futures import ThreadPoolExecutor
# import time
# import os

# from dotenv import load_dotenv

# from mainnet_launch.constants import ChainData, time_decorator

# load_dotenv()


# _etherscan_rate_limiter = asyncio.Semaphore(4)


# def get_nearest_block_before_timestamp(timestamp: int, chain: ChainData) -> int:
#     params = {
#         "chainid": str(chain.chain_id),
#         "module": "block",
#         "action": "getblocknobytime",
#         "timestamp": str(timestamp),
#         "closest": "before",
#         "apikey": os.environ.get("ETHERSCAN_API_KEY"),
#     }
#     url = "https://api.etherscan.io/v2/api"
#     attempts = 0
#     while attempts < 3:
#         try:
#             response = requests.get(url, params=params)
#             response.raise_for_status()
#             data = response.json()
#             return int(data["result"])
#         except Exception as e:
#             attempts += 1
#             time.sleep(attempts**2)
#     raise ValueError("Failed to fetch data")


# async def _get_nearest_block_before_timestamp_async(timestamp: int, chain: ChainData) -> int:
#     async with _etherscan_rate_limiter:
#         # Delay of 0.25 seconds ensures that, if tasks overlap, we make no more than 4 calls per second.
#         await asyncio.sleep(0.25)
#         loop = asyncio.get_running_loop()
#         result = await loop.run_in_executor(None, get_nearest_block_before_timestamp, timestamp, chain)
#         return result


# @time_decorator
# def get_nearest_blocks_before_timestamps(timestamps: list[int], chain: ChainData) -> dict[int, int]:
#     """
#     Given a list of timestamps, returns a dictionary mapping each timestamp
#     to its nearest block (before the timestamp)
#     """
#     results = {}
#     with ThreadPoolExecutor(max_workers=4) as executor:
#         # Submit each async task to run via asyncio.run in the executor.
#         future_map = {
#             ts: executor.submit(lambda t: asyncio.run(_get_nearest_block_before_timestamp_async(t, chain)), ts)
#             for ts in timestamps
#         }
#         for ts, future in future_map.items():
#             results[ts] = future.result()
#     return results


# if __name__ == "__main__":
#     from mainnet_launch.constants import ChainData, ETH_CHAIN

#     sample_timestamps = [1678638524 + (i * 10) for i in range(200)]
#     blocks_by_timestamp = get_nearest_blocks_before_timestamps(sample_timestamps, ETH_CHAIN)
