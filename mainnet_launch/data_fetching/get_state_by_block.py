import pandas as pd
import streamlit as st
import numpy as np
from functools import reduce
from datetime import datetime
from copy import deepcopy

from multicall import Multicall, Call
from sqlalchemy import select, func

import nest_asyncio
import asyncio
from mainnet_launch.app.app_config import STREAMLIT_IN_MEMORY_CACHE_TIME, SEMAPHORE_LIMITS_FOR_MULTICALL
from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    get_earliest_block_from_table_with_chain,
)
from mainnet_launch.database.should_update_database import should_update_table


from mainnet_launch.constants import ChainData, TokemakAddress, ALL_CHAINS, time_decorator

from mainnet_launch.database.schema.full import Blocks, Session

# needed to run these functions in a jupyter notebook
nest_asyncio.apply()


MULTICALL2_DEPLOYMENT_BLOCK = 12336033
MULTICALL_V3 = TokemakAddress(
    eth="0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696", base="0xcA11bde05977b3631167028862bE2a173976CA11"
)


def get_state_by_one_block(calls: list[Call], block: int, chain: ChainData):
    return asyncio.run(safe_get_raw_state_by_block_one_block(calls, int(block), chain))


async def safe_get_raw_state_by_block_one_block(calls: list[Call], block: int, chain: ChainData) -> dict:
    multicall = Multicall(calls=calls, block_id=block, _w3=chain.client, require_success=False, gas_limit=550_000_000)
    response = await multicall.coroutine()
    return response


def build_get_address_eth_balance_call(name: str, addr: str, chain: ChainData) -> Call:
    """Use the multicallV3 contract to get the normalized eth balance of an address"""
    return Call(
        MULTICALL_V3(chain),
        ["getEthBalance(address)(uint256)", addr],
        [(name, safe_normalize_with_bool_success)],
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


def get_raw_state_by_blocks(
    calls: list[Call],
    blocks: list[int],
    chain: ChainData,
    semaphore_limits: tuple[int] = SEMAPHORE_LIMITS_FOR_MULTICALL,
    include_block_number: bool = False,
) -> pd.DataFrame:
    return asyncio.run(
        async_safe_get_raw_state_by_block(
            calls,
            blocks,
            chain,
            semaphore_limits,
            include_block_number=include_block_number,
        )
    )


async def async_safe_get_raw_state_by_block(
    calls: list[Call],
    blocks: list[int],
    chain: ChainData,
    semaphore_limits: tuple[int] = SEMAPHORE_LIMITS_FOR_MULTICALL,
    include_block_number: bool = False,
    print_latency: bool = False,
) -> pd.DataFrame:
    """
    Fetch a DataFame of each call in calls for each block in blocks on chain


    note only works after the multicall_v3 contract was deployed
    block 12336033 (Apr-29-2021) on mainnet
    block 5022 (Jun-15-2023) on Base
    mostly a non issue but keep in mind that this only works on recent (within last 3 years) data

    """

    if not isinstance(calls, list):
        raise TypeError(f"{type(calls)=} is the wrong type")

    if len(calls) > 0:
        if not isinstance(calls[0], Call):
            raise TypeError(f"{type(calls[0])=} is the wrong type")

    if len(blocks) == 0:
        raise ValueError("Blocks cannot be empty")

    get_block_call, get_timestamp_call = _build_default_block_and_timestamp_calls(chain)

    all_multicalls = [
        Multicall(
            calls=[*calls, get_block_call, get_timestamp_call],
            block_id=int(block),
            _w3=chain.client,
            require_success=False,
            gas_limit=550_000_000,
        )
        for block in blocks
    ]

    semaphore = asyncio.Semaphore(semaphore_limits[0])

    latency_records = []

    async def _fetch_data(multicall: Multicall):
        async with semaphore:
            for attempt in range(5):
                start = datetime.now()
                try:

                    # response: dict = await multicall.coroutine()

                    response = await multicall.fetch_outputs(multicall.calls)
                    merged = {}
                    for d in response:
                        merged.update(d)
                    seconds_latency = (datetime.now() - start).microseconds / 1e6
                    latency_records.append(
                        {
                            "seconds_latency": seconds_latency,
                            "block": multicall.block_id,
                            "num_calls": len(multicall.calls),
                            "attempt": attempt,
                        }
                    )

                    return merged
                except Exception as e:
                    pass
                    # sleeping
                    await asyncio.sleep((attempt**2) * 0.1)

            pass
            raise ValueError("failed to fetch data")

            # maybe the rate limiting on there end?
            # if (e.args[0]["code"] == -32000) | (e.args[0]["code"] == 502): bad historical call, rate limited

    responses = await asyncio.gather(*[_fetch_data(m) for m in all_multicalls])

    if print_latency:
        print(pd.DataFrame(latency_records)["seconds_latency"].describe())
        print(pd.DataFrame(latency_records)["attempt"].describe())

    if len(responses) != len(blocks):
        raise ValueError(f"Unexpected length difference between multicall responses and blocks")

    df = _convert_multicall_responeses_to_df(responses, include_block_number)
    return df


def _convert_multicall_responeses_to_df(responses: list[dict], include_block_number: int):

    df = pd.DataFrame.from_records(responses)
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


# move to get raw state by block
def _constant_1(success, value) -> float:
    return 1.0


def make_dummy_1_call(name: str) -> Call:
    return Call(
        "0x000000000000000000000000000000000000dEaD",
        ["dummy()(uint256)"],
        [(name, _constant_1)],
    )


BLOCKS_TO_USE_TABLE = "BLOCKS_TO_USE_TABLE"


def _add_to_blocks_to_use_table():
    # drop_table(BLOCKS_TO_USE_TABLE)
    if should_update_table(BLOCKS_TO_USE_TABLE, max_latency=pd.Timedelta("1 hour")):
        for chain in ALL_CHAINS:
            highest_block = get_earliest_block_from_table_with_chain(BLOCKS_TO_USE_TABLE, chain)
            hour_blocks = _build_blocks_to_use_dont_clip(chain, start_block=highest_block, approx_num_blocks_per_day=48)

            # Retrieve the raw state for the given blocks, including block numbers
            df = get_raw_state_by_blocks([], hour_blocks, chain, include_block_number=True)
            df["chain"] = chain.name
            df = df.reset_index()
            write_dataframe_to_table(df, BLOCKS_TO_USE_TABLE)


# 180 seconds, too slow


def postgres_build_blocks_to_use(
    chain: ChainData, start_block: int | None = None, end_block: int | None = None
) -> list[int]:
    # TODO switch this to postgres version
    with Session.begin() as session:
        stmt = (
            select(func.max(Blocks.block).label("block"))
            .where(
                Blocks.chain_id == chain.chain_id,
                Blocks.block >= start_block,
                Blocks.block <= end_block,
            )
            .group_by(func.date_trunc("day", Blocks.datetime))
            .order_by(func.date_trunc("day", Blocks.datetime))
        )
        highest_block_in_each_day = session.scalars(stmt).all()

        return highest_block_in_each_day


# @st.cache_data(ttl=60 * 60)  # 1 hour
def build_blocks_to_use(chain: ChainData, start_block: int | None = None, end_block: int | None = None) -> list[int]:
    """Returns the highest block for day on chain stored in the postgres db"""

    start_block = chain.block_autopool_first_deployed if start_block is None else start_block
    end_block = 100_000_000 if end_block is None else end_block

    return postgres_build_blocks_to_use(chain, start_block, end_block)


def _build_blocks_to_use_dont_clip(
    chain: ChainData, start_block: int | None = None, end_block: int | None = None, approx_num_blocks_per_day: int = 48
) -> list[int]:
    """Returns a block approx every 6 hours. by default between when autopool was first deployed to the current block"""
    # this is not the number of seconds between blocks is not constant
    start_block = chain.block_autopool_first_deployed if start_block is None else start_block
    end_block = chain.client.eth.block_number if end_block is None else end_block
    blocks_hop = int(86400 / chain.approx_seconds_per_block) // approx_num_blocks_per_day

    blocks = [b for b in range(start_block, end_block, blocks_hop)]
    return blocks


if __name__ == "__main__":
    from mainnet_launch.constants import ETH_CHAIN

    b = ETH_CHAIN
    print(b)
