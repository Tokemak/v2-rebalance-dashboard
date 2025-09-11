from __future__ import annotations


import pandas as pd
from datetime import datetime

from multicall import Multicall, Call
from web3 import Web3

import nest_asyncio
import asyncio

from mainnet_launch.constants import (
    ChainData,
    TokemakAddress,
    ALL_CHAINS,
    ETH_CHAIN,
    DEAD_ADDRESS,
    SEMAPHORE_LIMITS_FOR_MULTICALL,
)
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache

# todo, refactor into a mulicall folder
# needed to run these functions in a jupyter notebook
nest_asyncio.apply()


class MulticallException(Exception):
    pass


MULTICALL_V3 = TokemakAddress(
    eth="0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696",
    base="0xcA11bde05977b3631167028862bE2a173976CA11",
    sonic="0xcA11bde05977b3631167028862bE2a173976CA11",
    name="multicall_v3",
)


def get_state_by_one_block(calls: list[Call], block: int, chain: ChainData):
    return asyncio.run(safe_get_raw_state_by_block_one_block(calls, int(block), chain))


async def safe_get_raw_state_by_block_one_block(calls: list[Call], block: int, chain: ChainData) -> dict:
    multicall = Multicall(
        calls=calls, block_id=block, _w3=ETH_CHAIN.client, require_success=False, gas_limit=550_000_000
    )

    # hacky, gets around Sonic not being in the old version of multicall

    multicall.w3 = chain.client
    multicall.chainid = chain.chain_id
    multicall.multicall_address = MULTICALL_V3(chain)
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
            _w3=ETH_CHAIN.client,
            require_success=False,
            gas_limit=550_000_000,
        )
        for block in blocks
    ]

    # hacky, gets around Sonic not being in the old version of multicall
    for m in all_multicalls:
        m.w3 = chain.client
        m.chainid = chain.chain_id
        m.multicall_address = MULTICALL_V3(chain)

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


def to_checksum_address_with_bool_success(success: bool, address: str) -> str | None:
    if success:
        try:
            return Web3.toChecksumAddress(address)
        except Exception as e:
            raise MulticallException(f"Failed to convert {address} to checksum address") from e
    return None


def identity_function(value):
    return value


# move to get raw state by block
def _constant_1(success, value) -> float:
    return 1.0


def make_dummy_1_call(name: str) -> Call:
    return Call(
        DEAD_ADDRESS,
        ["dummy()(uint256)"],
        [(name, _constant_1)],
    )


def build_blocks_to_use(chain: ChainData, start_block: int | None = None, end_block: int | None = None) -> list[int]:
    """Returns the highest block for day on chain stored in the postgres db"""

    start_block = chain.block_autopool_first_deployed if start_block is None else start_block
    end_block = 100_000_000 if end_block is None else end_block

    query = f"""SELECT DISTINCT ON (DATE_TRUNC('day', datetime))
        DATE_TRUNC('day', datetime) AS day,
        block AS max_block,
        datetime AS datetime_of_max
    FROM blocks
    WHERE chain_id = {chain.chain_id}
    AND block BETWEEN {start_block} AND {end_block}
    ORDER BY DATE_TRUNC('day', datetime), block DESC;"""

    block_df = _exec_sql_and_cache(query)
    if block_df.empty:
        return []
    else:
        # exclude the last day since is is not certain that the block is the highest block of the day
        # since there can be more hours left in the day
        block_df = block_df[block_df["datetime_of_max"] != block_df["datetime_of_max"].max()]
        blocks_to_use = block_df["max_block"].astype(int).to_list()
        return blocks_to_use


if __name__ == "__main__":
    from mainnet_launch.constants import ETH_CHAIN, BASE_CHAIN, ALL_CHAINS

    for c in ALL_CHAINS:
        print(get_raw_state_by_blocks([], [20_000_000], chain=c, include_block_number=True))
