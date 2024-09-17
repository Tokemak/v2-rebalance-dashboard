import pandas as pd
from multicall import Multicall, Call
import streamlit as st

import nest_asyncio
import asyncio


from mainnet_launch.constants import eth_client

nest_asyncio.apply()


MULTICALL2_DEPLOYMENT_BLOCK = 12336033
multicall_v3 = "0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696"


def get_state_by_one_block(calls: list[Call], block: int):
    return asyncio.run(safe_get_raw_state_by_block_one_block(calls, block))


async def safe_get_raw_state_by_block_one_block(calls: list[Call], block: int):
    # nice for testing
    multicall = Multicall(calls=calls, block_id=block, _w3=eth_client, require_success=False)
    response = await multicall.coroutine()
    return response


def build_get_address_eth_balance_call(name: str, addr: str) -> Call:
    """Use the multicallV3 contract to get the normalized eth balance of an address"""
    return Call(
        multicall_v3,
        ["getEthBalance(address)(uint256)", addr],
        [(name, safe_normalize_with_bool_success)],
    )


def _build_default_block_and_timestamp_calls():
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


def get_raw_state_by_blocks(
    calls: list[Call],
    blocks: list[int],
    semaphore_limits: int = (500, 200, 50, 20, 2),  # Increased limits
    include_block_number: bool = False,
) -> pd.DataFrame:
    return asyncio.run(async_safe_get_raw_state_by_block(calls, blocks, semaphore_limits, include_block_number))


async def async_safe_get_raw_state_by_block(
    calls: list[Call],
    blocks: list[int],
    semaphore_limits: int = (500, 200, 50, 20, 2),  # Increased limits
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
        print(
            f"failed to fetch any data. consider trying again if expected to get data, but with a smaller semaphore_limit"
        )
        print(f"{len(blocks)=} {blocks[0]=} {blocks[-1]=}")
        print(f"{calls=}")

    df.set_index("timestamp", inplace=True)
    df.index = pd.to_datetime(df.index, unit="s")
    df.sort_index(inplace=True)
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


def build_blocks_to_use(use_mainnet: bool = True) -> list[int]:
    """Returns daily blocks since deployement"""
    current_block = eth_client.eth.block_number

    start_block = 20722910 if use_mainnet else 20262439

    # Average block time in seconds
    block_time_seconds = 13.15
    # Calculate blocks per day
    blocks_per_day = int(86400 / block_time_seconds)

    # Generate blocks with an interval of 1 block per day
    blocks = [b for b in range(current_block, start_block, -blocks_per_day)]
    blocks.reverse()
    return blocks
