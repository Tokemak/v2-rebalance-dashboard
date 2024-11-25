import pandas as pd
from multicall import Multicall, Call
import streamlit as st

import nest_asyncio
import asyncio
import hashlib
import json

import inspect
from mainnet_launch.constants import CACHE_TIME, ChainData, TokemakAddress, time_decorator, ETH_CHAIN, BASE_CHAIN
from mainnet_launch.data_fetching.databases import MULTICALL_LOGS_DB
from mainnet_launch.data_fetching.multicall_ import (
    batch_insert_multicall_logs,
    batch_load_multicall_logs_if_exists,
    insert_multicall_log,
    get_multicall_log_if_exists,
)
import sqlite3

from threading import Lock
from dataclasses import dataclass
from functools import lru_cache, cached_property

import concurrent.futures

from mainnet_launch.constants import ETH_CHAIN, AUTO_LRT, BASE_ETH, BASE_CHAIN, WETH, AUTO_ETH
import random

nest_asyncio.apply()  # needed to run these functions in a jupyter notebook


MULTICALL_V3 = TokemakAddress(
    eth="0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696", base="0xcA11bde05977b3631167028862bE2a173976CA11"
)


@time_decorator
def get_state_by_one_block(calls: list[Call], block: int, chain: ChainData):
    return asyncio.run(safe_get_raw_state_by_block_one_block(calls, block, chain))


async def safe_get_raw_state_by_block_one_block(calls: list[Call], block: int, chain: ChainData):
    multicall = Multicall(calls=calls, block_id=block, _w3=chain.client, require_success=False)
    multicall_hash = _multicall_to_db_hash(multicall)
    response = _get_multicall_log_if_exists(multicall_hash)
    if response is not None:
        return response

    response = await multicall.coroutine()
    _insert_multicall_log(multicall_hash, response)
    return response


def _build_default_block_and_timestamp_calls(chain: ChainData):
    get_block_call = Call(
        MULTICALL_V3(chain),
        ["getBlockNumber()(uint256)"],
        [("block", identity_with_bool_success)],
    )
    get_timestamp_call = Call(
        MULTICALL_V3(chain),
        ["getCurrentBlockTimestamp()(uint256)"],
        [("timestamp", identity_with_bool_success)],
    )
    return get_block_call, get_timestamp_call


@time_decorator
def get_raw_state_by_blocks(
    calls: list[Call],
    blocks: list[int],
    chain: ChainData,
    semaphore_limits: tuple = (500, 200, 50, 20, 2),  # Increased limits
    include_block_number: bool = False,
) -> pd.DataFrame:
    """Fetch a DataFrame of each call in calls for each block in blocks quickly using caching."""
    return asyncio.run(async_safe_get_raw_state_by_blocks(calls, blocks, chain, semaphore_limits, include_block_number))


def _fetch_already_cached_responses(calls: list[Call], blocks: list[int], chain: ChainData):

    get_block_call, get_timestamp_call = _build_default_block_and_timestamp_calls(chain)

    # Prepare Multicall objects for all blocks
    all_multicalls = [
        Multicall(
            calls=[*calls, get_block_call, get_timestamp_call],
            block_id=int(block),
            _w3=chain.client,
            require_success=False,
        )
        for block in blocks
    ]

    multicall_db_hashes = _multicall_to_db_hash_parallel(all_multicalls, 10)
    cached_hash_to_response = _batch_get_multicall_logs(multicall_db_hashes)

    multicalls_left_to_fetch = [m for m in all_multicalls if _multicall_to_db_hash(m) not in cached_hash_to_response]

    return cached_hash_to_response, multicalls_left_to_fetch, all_multicalls


async def async_safe_get_raw_state_by_blocks(
    calls: list[Call],
    blocks: list[int],
    chain: ChainData,
    semaphore_limits: tuple = (500, 200, 50, 20, 2),
    include_block_number: bool = False,
) -> pd.DataFrame:
    """
    Fetch a DataFrame of each call in `calls` for each block in `blocks` efficiently using caching.
    """
    if (len(blocks)) != len(set(blocks)):
        raise ValueError("No duplicates in blocks")

    highest_finalized_block = _get_highest_finalized_block(chain)

    cached_hash_to_response, multicalls_left_to_fetch, all_multicalls = _fetch_already_cached_responses(
        calls, blocks, chain
    )

    db_hashes_already_cached = list(cached_hash_to_response.keys())

    lock = Lock()
    for max_http_calls_per_second in semaphore_limits:
        if len(multicalls_left_to_fetch) == 0:
            break

        semaphore = asyncio.Semaphore(max_http_calls_per_second)
        multicalls_that_failed = []

        async def _fetch_data(multicall: Multicall):
            async with semaphore:
                try:
                    response = await multicall.coroutine()
                    if response is None:
                        raise ValueError("response should not be None")
                    db_hash = _multicall_to_db_hash(multicall)
                    # need a lock becuase updating a dictionary is not thread safe
                    with lock:
                        cached_hash_to_response[db_hash] = response
                except Exception as e:
                    print(e)
                    multicalls_that_failed.append(multicall)

                    # if (e.args[0]["code"] == -32000) | (e.args[0]["code"] == 502): bad historical call, rate limited

        tasks = [_fetch_data(m) for m in multicalls_left_to_fetch]
        await asyncio.gather(*tasks)
        multicalls_left_to_fetch = [m for m in multicalls_that_failed]

    all_responses = [v for k, v in cached_hash_to_response.items()]
    # only cache them if the block is less than the current lbock
    finalized_responses_to_cache: list[tuple[str, dict[str, any]]] = [
        (db_hash, response)
        for db_hash, response in cached_hash_to_response.items()
        if (response["block"] <= highest_finalized_block) and (db_hash not in db_hashes_already_cached)
    ]

    _batch_insert_multicall_logs(finalized_responses_to_cache)

    df = pd.DataFrame(all_responses)
    df.set_index("timestamp", inplace=True)
    df.index = pd.to_datetime(df.index, unit="s", utc=True)
    df.sort_index(inplace=True)

    df["block"] = df["block"].astype(int)
    if not include_block_number:
        df.drop(columns="block", inplace=True)
    return df


def safe_normalize_with_bool_success(success: int, value: int):
    if success:
        return int(value) / 1e18
    return None


def safe_normalize_6_with_bool_success(success: int, value: int):
    if success:
        return int(value) / 1e6
    return None


def to_str_with_bool_success(success, value):
    if success:
        return str(value)
    return None


def identity_with_bool_success(success, value):
    if success:
        return value
    return None


def identity_function(value):
    return value


def build_blocks_to_use(
    chain: ChainData, start_block: int | None = None, end_block: int | None = None, approx_num_blocks_per_day: int = 1
) -> list[int]:
    # just start from the start block, and go until you hit the end block instead of going backwards
    """Returns a block approx once per day. by default between when autopool was first deployed to the highest finalized block"""
    start_block = chain.block_autopool_first_deployed if start_block is None else start_block

    end_block = _get_highest_finalized_block() if end_block is None else end_block
    blocks_hop = int(86400 / chain.approx_seconds_per_block) // approx_num_blocks_per_day
    blocks = [b for b in range(start_block, end_block, blocks_hop)]
    return blocks


def _get_highest_finalized_block(chain: ChainData):
    return chain.client.eth.get_block("latest").number - (500)  # close enough on the side of caution


def _make_calls():

    weth_bal_of_AUTO_LRT = Call(
        WETH(ETH_CHAIN),
        ["balanceOf(address)(uint256)", AUTO_LRT.autopool_eth_addr],
        [("AUTO_LRT_weth_bal", safe_normalize_with_bool_success)],
    )

    weth_bal_of_AUTO_eth = Call(
        WETH(ETH_CHAIN),
        ["balanceOf(address)(uint256)", AUTO_ETH.autopool_eth_addr],
        [("AUTO_ETH_weth_bal", safe_normalize_with_bool_success)],
    )

    return [weth_bal_of_AUTO_eth, weth_bal_of_AUTO_LRT]


def get_multicall_log_if_exists(multicall_db_hash: str) -> dict | None:
    """Retrieve a log from the multicall_logs table by hash."""
    with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT response FROM multicall_logs WHERE multicall_hash = ?
            """,
            (f"{multicall_db_hash}",),
        )
        row = cursor.fetchone()
        if row is None:
            print("failed to load", multicall_db_hash)
            return None
        print("loaded", multicall_db_hash)
        return json.loads(row[0]) if row[0] else None  # Deserialize JSON response


def insert_multicall_log(multicall_db_hash: int, response: dict):
    """Insert a new log into the multicall_logs table."""
    with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO multicall_logs (multicall_hash, response)
            VALUES (?, ?)
            """,
            (f"{multicall_db_hash}", json.dumps(response)),  # Convert hash to hex string
        )
        conn.commit()
        print("saved", multicall_db_hash)


def batch_insert_multicall_logs(finalized_responses_to_cache):
    """
    Batch insert multiple multicall logs into the cache.

    Parameters:
        multicall_hashes (list[str]): List of multicall hash strings.
        responses (list[dict]): Corresponding list of response dictionaries.
    """
    if len(finalized_responses_to_cache) > 0:
        # only touch the db if there are more data to cache
        hashes_to_insert = [d[0] for d in finalized_responses_to_cache]
        responses_to_insert = [d[1] for d in finalized_responses_to_cache]
        with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
            cursor = conn.cursor()
            cursor.executemany(
                """
                INSERT OR REPLACE INTO multicall_logs (multicall_hash, response)
                VALUES (?, ?)
                """,
                zip(hashes_to_insert, (json.dumps(response) for response in responses_to_insert)),
            )
            conn.commit()
            print(f"Inserted {len(responses_to_insert)} multicall responses into cache.")


def batch_load_multicall_logs_if_exists(multicall_hashes: list[str]) -> dict[str, dict | None]:
    """
    Batch retrieve multicall responses from the cache.

    Parameters:
        multicall_hashes (list[str]): List of multicall hash strings.

    """
    if len(multicall_hashes) == 0:
        return {}

    with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
        cursor = conn.cursor()
        placeholders = ",".join(["?"] * len(multicall_hashes))
        query = f"SELECT multicall_hash, response FROM multicall_logs WHERE multicall_hash IN ({placeholders})"
        cursor.execute(query, multicall_hashes)
        rows = cursor.fetchall()
        print(print(f"loaded {len(rows)} multicall responses of {len(multicall_hashes)} needed"))

    # only valid jsons should be here so we should fail on trying to load one that does not work
    cached_hash_to_response = {row[0]: json.loads((row[1])) for row in rows}
    return cached_hash_to_response


def _test_get_state_once():
    print(ETH_CHAIN.client.eth.chainId)
    calls = _make_calls()
    block = 21238611 + random.randint(0, 10000)

    state1 = get_state_by_one_block(calls, block, ETH_CHAIN)
    state2 = get_state_by_one_block(calls, block, ETH_CHAIN)
    assert state1 == state2


def _test_get_many_states():
    print(ETH_CHAIN.client.eth.chainId)
    calls = _make_calls()
    block = random.randint(10_000_000, 21_000_000)
    blocks = [block + i for i in range(100)]

    # intentionally slow to show speed up
    df1 = get_raw_state_by_blocks(calls, blocks, ETH_CHAIN, semaphore_limits=(1, 1, 1))

    df2 = get_raw_state_by_blocks(calls, blocks, ETH_CHAIN, semaphore_limits=(1, 1, 1))

    assert df1.equals(df2)


if __name__ == "__main__":
    _test_get_state_once()
    _test_get_many_states()
