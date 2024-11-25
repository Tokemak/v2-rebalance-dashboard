import pandas as pd
from multicall import Multicall, Call
import streamlit as st

import nest_asyncio
import asyncio
import hashlib
import json

import inspect
from mainnet_launch.constants import CACHE_TIME, ChainData, TokemakAddress, time_decorator, ETH_CHAIN, BASE_CHAIN
from mainnet_launch.data_fetching.databases import (
    MULTICALL_LOGS_DB,
    batch_insert_multicall_logs,
    batch_load_multicall_logs_if_exists,
)
from mainnet_launch.data_fetching.multicall_hashing import calls_and_blocks_to_db_hashes
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


def _fetch_already_cached_responses(
    calls: list[Call], blocks: list[int], chain: ChainData
) -> dict[str, dict[str, Multicall | None]]:

    get_block_call, get_timestamp_call = _build_default_block_and_timestamp_calls(chain)
    calls_to_fetch = [get_block_call, get_timestamp_call, *calls]

    db_hash_to_multicall_and_response = calls_and_blocks_to_db_hashes(calls_to_fetch, blocks, chain)
    db_hash_to_multicall_and_response = batch_load_multicall_logs_if_exists(db_hash_to_multicall_and_response)

    return db_hash_to_multicall_and_response


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

    db_hash_to_multicall_and_response = _fetch_already_cached_responses(calls, blocks, chain)

    # updating a dictionary is not thread safe so we need to only update it when a thread has aquired the lock
    lock = Lock()

    for max_http_calls_per_second in semaphore_limits:
        semaphore = asyncio.Semaphore(max_http_calls_per_second)
        db_hashes_left_to_fetch = [
            (db_hash, multicall_and_maybe_response["multicall"])
            for db_hash, multicall_and_maybe_response in db_hash_to_multicall_and_response.items()
            if "response" not in multicall_and_maybe_response
        ]
        if len(db_hashes_left_to_fetch) == 0:
            # if there are no more responses left to fetch exit early
            break

        async def _fetch_data(db_hash: str, multicall: Multicall):
            async with semaphore:
                try:
                    response = await multicall.coroutine()
                    with lock:
                        db_hash_to_multicall_and_response[db_hash]["response"] = response
                except Exception as e:
                    # rate limiting http fails etc
                    # if (e.args[0]["code"] == -32000) | (e.args[0]["code"] == 502): bad historical call, rate limited
                    # multicalls_that_failed.append((db_hash, multicall))
                    print(e)
                    pass

        # upate the db_hash_to_multicall_and_response to have all the responses
        await asyncio.gather(*[_fetch_data(db_hash, multicall) for db_hash, multicall in db_hashes_left_to_fetch])

    batch_insert_multicall_logs(db_hash_to_multicall_and_response, highest_finalized_block)

    if any(
        [
            multicall_and_response["response"] is None
            for db_hash, multicall_and_response in db_hash_to_multicall_and_response.items()
        ]
    ):
        raise ValueError("a response is None when it should not be")

    all_responses = [
        multicall_and_response["response"]
        for db_hash, multicall_and_response in db_hash_to_multicall_and_response.items()
    ]

    df = pd.DataFrame(all_responses)
    df.set_index("timestamp", inplace=True)
    df.index = pd.to_datetime(df.index, unit="s", utc=True)
    df.sort_index(inplace=True)

    df["block"] = df["block"].astype(int)
    if not include_block_number:
        df.drop(columns="block", inplace=True)
    return df


async def safe_get_raw_state_by_block_one_block(calls: list[Call], block: int, chain: ChainData):
    df = await async_safe_get_raw_state_by_blocks(calls, [block], chain)
    response = df.to_records("orai")


@time_decorator
def get_state_by_one_block(calls: list[Call], block: int, chain: ChainData):
    return asyncio.run(safe_get_raw_state_by_block_one_block(calls, block, chain))


# todo move these
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


def _test_get_state_once():
    calls = _build_default_block_and_timestamp_calls(ETH_CHAIN)
    block = 21238611 + random.randint(0, 10000)

    state1 = get_state_by_one_block(calls, block, ETH_CHAIN)
    state2 = get_state_by_one_block(calls, block, ETH_CHAIN)
    assert state1 == state2


def _test_get_many_states():
    balance_of_calls = [
        Call(
            MULTICALL_V3(ETH_CHAIN),
            ["bad_call(uint256)(uint256)", i],
            [(str(i), identity_with_bool_success)],
        )
        for i in range(100)
    ]

    print(ETH_CHAIN.client.eth.chainId)
    calls = _build_default_block_and_timestamp_calls(ETH_CHAIN)
    blocks = [14211989 + i for i in range(2000)]
    # good enough, for what I'm trying to do
    # later can consider optimizing how the response jsons are stored
    # 100 bad calls
    # successfully read len(rows)= 0 of len(len(db_hashes_to_fetch)=2000)
    # successfully wrote  len(hashes_to_insert)= 2000
    # get_raw_state_by_blocks took 68.3048 seconds.
    # successfully read len(rows)= 2000 of len(len(db_hashes_to_fetch)=2000)
    # successfully wrote  len(hashes_to_insert)= 2000
    # get_raw_state_by_blocks took 0.2380 seconds.

    # naive speed on 2k  blocks
    # get_raw_state_by_blocks took 10.2173 seconds.
    # get_raw_state_by_blocks took 0.1369 seconds.

    # intentionally slow to show speed up
    df1 = get_raw_state_by_blocks(
        [*balance_of_calls, *calls], blocks, ETH_CHAIN, semaphore_limits=(50, 25, 1), include_block_number=True
    )

    df2 = get_raw_state_by_blocks(
        [*balance_of_calls, *calls], blocks, ETH_CHAIN, semaphore_limits=(50, 25, 1), include_block_number=True
    )
    print(df1.head())
    print(df2.tail())

    assert df1.equals(df2), "returned dfs are differnet"
    pass


if __name__ == "__main__":
    # _test_get_state_once()
    _test_get_many_states()
