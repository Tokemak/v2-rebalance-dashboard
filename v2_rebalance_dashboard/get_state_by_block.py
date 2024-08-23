import asyncio
import pandas as pd
from multicall import Multicall, Call
import numpy as np

from os import environ
from web3 import Web3

import nest_asyncio
import asyncio

nest_asyncio.apply()

ALCHEMY_URL = environ["ALCHEMY_URL"]
eth_client = Web3(Web3.HTTPProvider(ALCHEMY_URL))
MULTICALL2_DEPLOYMENT_BLOCK = 12336033


def sync_get_raw_state_by_block_one_block(calls: list[Call], block: int):
    return asyncio.run(safe_get_raw_state_by_block_one_block(calls, block))


async def safe_get_raw_state_by_block_one_block(calls: list[Call], block: int):
    # nice for testing
    multicall = Multicall(calls=calls, block_id=block, _w3=eth_client, require_success=False)
    response = await multicall.coroutine()
    return response


def _build_default_block_and_timestamp_calls():
    multicall_v3 = "0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696"
    get_block_call = Call(
        multicall_v3,
        ["getBlockNumber()(uint256)"],
        [("block", identity_with_bool_success)],
    )

    get_timestamp_call = Call(
        multicall_v3,
        ["getCurrentBlockTimestamp()(uint256)"],
        [("timestamp", identity_with_bool_success)],
    )
    return get_block_call, get_timestamp_call


def _data_fetch_builder(semaphore: asyncio.Semaphore, responses: list, failed_multicalls: list):
    async def _fetch_data(multicall: Multicall):
        async with semaphore:
            try:
                response = await multicall.coroutine()
                responses.append(response)
            except Exception:
                # if (e.args[0]["code"] == -32000) | (e.args[0]["code"] == 502): bad historical call, rate limited
                failed_multicalls.append(multicall)

    return _fetch_data


def sync_safe_get_raw_state_by_block(
    calls: list[Call],
    blocks: list[int],
    semaphore_limits: int = (300, 100, 30, 10, 1),
    include_block_number: bool = False,
) -> pd.DataFrame:
    return asyncio.run(async_safe_get_raw_state_by_block(calls, blocks, semaphore_limits, include_block_number))


async def async_safe_get_raw_state_by_block(
    calls: list[Call],
    blocks: list[int],
    semaphore_limits: int = (300, 100, 30, 10, 1),
    include_block_number: bool = False,
) -> pd.DataFrame:
    """
    Fetch a DataFame of each call in calls for each block in blocks fast
    """

    if any(block <= MULTICALL2_DEPLOYMENT_BLOCK for block in blocks):
        raise TypeError("all blocks must > 12336033")

    get_block_call, get_timestamp_call = _build_default_block_and_timestamp_calls()
    pending_multicalls = [
        Multicall(
            calls=[*calls, get_block_call, get_timestamp_call],
            block_id=b,
            _w3=eth_client,
            require_success=False,
        )
        for b in blocks
    ]

    responses = []
    failed_multicalls = []
    calls_remaining = [m for m in pending_multicalls]
    for semaphore_limit in semaphore_limits:
        # print(f"{len(calls_remaining)=} {semaphore_limit=}")
        # make a lot of calls very fast, then slowly back off and remake the calls that failed
        semaphore = asyncio.Semaphore(semaphore_limit)
        failed_multicalls = []
        _ratelimited_async_data_fetcher = _data_fetch_builder(semaphore, responses, failed_multicalls)
        await asyncio.gather(*[_ratelimited_async_data_fetcher(m) for m in calls_remaining])

        calls_remaining = [f for f in failed_multicalls]
        if len(calls_remaining) == 0:
            break

    df = pd.DataFrame.from_records(responses)
    if len(df) == 0:
        raise ValueError("failed to fetch any data, df is empty")
    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)
    df.index = pd.to_datetime(df.index, unit="s")
    if not include_block_number:
        df.drop(columns="block", inplace=True)
    return df


def safe_normalize_with_bool_success(success: int, value: int):
    if success:
        return int(value) / 1e18
    return None


def identity_with_bool_success(success, value):
    if success:
        return value
    return None


def build_blocks_to_use():
    current_block = eth_client.eth.block_number
    start_block = 20162439  # TODO, get a better method for this. blocks are not perfectly line dup
    approx_blocks_per_day = 7100
    blocks = [b for b in range(start_block, current_block, approx_blocks_per_day)]
    # good enough for now, fix later use etherscan
    return blocks
