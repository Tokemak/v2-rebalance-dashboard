import pandas as pd
from multicall import Multicall, Call

import nest_asyncio
import asyncio

from v2_rebalance_dashboard.constants import eth_client

nest_asyncio.apply()


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


import pandas as pd
from multicall import Multicall, Call
import asyncio
from v2_rebalance_dashboard.constants import eth_client

MULTICALL2_DEPLOYMENT_BLOCK = 12336033

async def async_safe_get_raw_state_by_block(
    calls: list[Call],
    blocks: list[int],
    semaphore_limit: int = 100,
    include_block_number: bool = False,
) -> pd.DataFrame:
    if any(block <= MULTICALL2_DEPLOYMENT_BLOCK for block in blocks):
        raise ValueError(f"All blocks must be > {MULTICALL2_DEPLOYMENT_BLOCK}")

    get_block_call, get_timestamp_call = _build_default_block_and_timestamp_calls()
    semaphore = asyncio.Semaphore(semaphore_limit)

    async def fetch_data(block):
        async with semaphore:
            multicall = Multicall(
                calls=[*calls, get_block_call, get_timestamp_call],
                block_id=block,
                _w3=eth_client,
                require_success=False,
            )
            return await multicall.coroutine()

    responses = await asyncio.gather(*[fetch_data(block) for block in blocks], return_exceptions=True)
    valid_responses = [r for r in responses if not isinstance(r, Exception)]

    if not valid_responses:
        raise ValueError("Failed to fetch any data, all requests failed")

    df = pd.DataFrame.from_records(valid_responses)
    df.set_index("timestamp", inplace=True)
    df.sort_index(inplace=True)
    df.index = pd.to_datetime(df.index, unit="s")
    if not include_block_number:
        df.drop(columns="block", inplace=True)
    return df

def sync_safe_get_raw_state_by_block(
    calls: list[Call],
    blocks: list[int],
    semaphore_limit: int = 100,
    include_block_number: bool = False,
) -> pd.DataFrame:
    return asyncio.run(async_safe_get_raw_state_by_block(calls, blocks, semaphore_limit, include_block_number))


def safe_normalize_with_bool_success(success: int, value: int):
    if success:
        return int(value) / 1e18
    return None


def identity_with_bool_success(success, value):
    if success:
        return value
    return None


def identity_function(value):
    return value


def build_blocks_to_use():
    current_block = eth_client.eth.block_number
    start_block = 20262439  # Start block number

    # Average block time in seconds
    block_time_seconds = 13.15
    # Calculate blocks per day
    blocks_per_day = int(86400 / block_time_seconds)

    # Generate blocks with an interval of 1 block per day
    blocks = [b for b in range(start_block, current_block, blocks_per_day)]
    return blocks