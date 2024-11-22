import pandas as pd
from multicall import Multicall, Call
import streamlit as st

import nest_asyncio
import asyncio
import hashlib
import json

import inspect
from mainnet_launch.constants import CACHE_TIME, ChainData, TokemakAddress, time_decorator
from mainnet_launch.data_fetching.databases import MULTICALL_LOGS_DB
import sqlite3

# needed to run these functions in a jupyter notebook
nest_asyncio.apply()


MULTICALL_V3 = TokemakAddress(
    eth="0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696", base="0xcA11bde05977b3631167028862bE2a173976CA11"
)


def _call_to_string(call: Call) -> str:
    returns_functions = [(name, inspect.getsource(handling_function)) for name, handling_function in call.returns]
    return str((call.target.lower(), call.data, call.function, returns_functions))


def _multicall_to_db_hash(multicall: Multicall) -> str:
    # returns the str hash of this unique set of calls on this block on this chain, with the same require success flag
    call_identifier_strings = str([_call_to_string(call) for call in multicall.calls])
    data = f"""
    {multicall.block_id=}
    {multicall.chainid=}
    {multicall.require_success=}
    {multicall.multicall_address.lower()=}
    {call_identifier_strings=}
    """
    sha256_hash = hashlib.sha256(data.encode()).hexdigest()
    return sha256_hash

@time_decorator
def get_state_by_one_block(calls: list[Call], block: int, chain: ChainData):
    return asyncio.run(safe_get_raw_state_by_block_one_block(calls, block, chain))

# TODO rename this
async def safe_get_raw_state_by_block_one_block(calls: list[Call], block: int, chain: ChainData):
    multicall = Multicall(calls=calls, block_id=block, _w3=chain.client, require_success=False)
    multicall_hash = _multicall_to_db_hash(multicall)
    response = _get_multicall_log_if_exists(multicall_hash)
    if response is not None:
        return response

    response = await multicall.coroutine()
    _insert_multicall_log(multicall_hash, response)
    return response




def _get_multicall_log_if_exists(multicall_hash: str) -> dict | None:
    """Retrieve a log from the multicall_logs table by hash."""
    with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT response FROM multicall_logs WHERE multicall_hash = ?
            """,
            (f"{multicall_hash}",),
        )
        row = cursor.fetchone()
        if row is None:
            print('failed to load', multicall_hash)
            return None
        print('loaded', multicall_hash)
        return json.loads(row[0]) if row[0] else None  # Deserialize JSON response


def _insert_multicall_log(multicall_hash: int, response: dict):
    """Insert a new log into the multicall_logs table."""
    with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO multicall_logs (multicall_hash, response)
            VALUES (?, ?)
            """,
            (f"{multicall_hash}", json.dumps(response)),  # Convert hash to hex string
        )
        conn.commit()
        print('saved', multicall_hash)


def _tester():

    from mainnet_launch.constants import ETH_CHAIN, AUTO_LRT, BASE_ETH, BASE_CHAIN, WETH, AUTO_ETH

    def safe_normalize_with_bool_success(success: int, value: int):
        if success:
            return int(value) / 1e18
        return None

    block = 21238611

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

    calls = [weth_bal_of_AUTO_eth, weth_bal_of_AUTO_LRT,weth_bal_of_AUTO_LRT]
    
    for i in range(10):
        calls = [weth_bal_of_AUTO_eth  for _ in range(i)]
        a = get_state_by_one_block(calls, block, ETH_CHAIN)
        print(a)
        
    for i in range(10):
        calls = [weth_bal_of_AUTO_eth  for _ in range(i)]
        a = get_state_by_one_block(calls, block, ETH_CHAIN)
        print(a)
    
#
_tester()















# async def safe_get_raw_state_by_block_one_block(calls: list[Call], block: int, chain: ChainData):
#     # nice for testing
#     multicall = Multicall(calls=calls, block_id=block, _w3=chain.client, require_success=False)
#     response = await multicall.coroutine()
#     return response


# def build_get_address_eth_balance_call(name: str, addr: str, chain: ChainData) -> Call:
#     """Use the multicallV3 contract to get the normalized eth balance of an address"""
#     return Call(
#         MULTICALL_V3(chain),
#         ["getEthBalance(address)(uint256)", addr],
#         [(name, safe_normalize_with_bool_success)],
#     )


# def _build_default_block_and_timestamp_calls(chain: ChainData):
#     get_block_call = Call(
#         MULTICALL_V3(chain),
#         ["getBlockNumber()(uint256)"],
#         [("block", identity_with_bool_success)],
#     )

#     get_timestamp_call = Call(
#         MULTICALL_V3(chain),
#         ["getCurrentBlockTimestamp()(uint256)"],
#         [("timestamp", identity_with_bool_success)],
#     )
#     return get_block_call, get_timestamp_call


# def _data_fetch_builder(semaphore: asyncio.Semaphore, responses: list, failed_multicalls: list):
#     async def _fetch_data(multicall: Multicall):
#         async with semaphore:
#             try:
#                 response = await multicall.coroutine()
#                 responses.append(response)
#             except Exception:
#                 # if (e.args[0]["code"] == -32000) | (e.args[0]["code"] == 502): bad historical call, rate limited
#                 failed_multicalls.append(multicall)

#     return _fetch_data


# def get_raw_state_by_blocks(
#     calls: list[Call],
#     blocks: list[int],
#     chain: ChainData,
#     semaphore_limits: int = (500, 200, 50, 20, 2),  # Increased limits
#     include_block_number: bool = False,
# ) -> pd.DataFrame:
#     return asyncio.run(async_safe_get_raw_state_by_block(calls, blocks, chain, semaphore_limits, include_block_number))


# async def async_safe_get_raw_state_by_block(
#     calls: list[Call],
#     blocks: list[int],
#     chain: ChainData,
#     semaphore_limits: int = (500, 200, 50, 20, 2),  # Increased limits
#     include_block_number: bool = False,
# ) -> pd.DataFrame:
#     """
#     Fetch a DataFame of each call in calls for each block in blocks fast
#     """
#     blocks_as_ints = [int(b) for b in blocks]
#     # note only works after the multicall_v3 contract was deployed
#     # block 12336033 (Apr-29-2021) on mainnet
#     # block 5022 (Jun-15-2023) on Base
#     # mostly a non issue but keep in mind that this only works on recent (within last 3 years) data

#     get_block_call, get_timestamp_call = _build_default_block_and_timestamp_calls(chain)
#     pending_multicalls = [
#         Multicall(
#             calls=[*calls, get_block_call, get_timestamp_call],
#             block_id=int(block),
#             _w3=chain.client,
#             require_success=False,
#         )
#         for block in blocks_as_ints
#     ]

#     responses = []
#     failed_multicalls = []
#     calls_remaining = [m for m in pending_multicalls]
#     for semaphore_limit in semaphore_limits:
#         # print(f"{len(calls_remaining)=} {semaphore_limit=}")
#         # make a lot of calls very fast, then slowly back off and remake the calls that failed
#         semaphore = asyncio.Semaphore(semaphore_limit)
#         failed_multicalls = []
#         _ratelimited_async_data_fetcher = _data_fetch_builder(semaphore, responses, failed_multicalls)
#         await asyncio.gather(*[_ratelimited_async_data_fetcher(m) for m in calls_remaining])

#         calls_remaining = [f for f in failed_multicalls]
#         if len(calls_remaining) == 0:
#             break

#     df = pd.DataFrame.from_records(responses)
#     if len(df) == 0:
#         print(
#             f"failed to fetch any data. consider trying again if expected to get data, but with a smaller semaphore_limit"
#         )
#         print(f"{len(blocks_as_ints)=} {blocks_as_ints[0]=} {blocks_as_ints[-1]=}")
#         print(f"{calls=}")

#     df.set_index("timestamp", inplace=True)
#     df.index = pd.to_datetime(df.index, unit="s", utc=True)
#     df.sort_index(inplace=True)
#     df["block"] = df["block"].astype(int)
#     if not include_block_number:
#         df.drop(columns="block", inplace=True)
#     return df


# def safe_normalize_with_bool_success(success: int, value: int):
#     if success:
#         return int(value) / 1e18
#     return None


# def safe_normalize_6_with_bool_success(success: int, value: int):
#     if success:
#         return int(value) / 1e6
#     return None


# def to_str_with_bool_success(success, value):
#     if success:
#         return str(value)
#     return None


# def identity_with_bool_success(success, value):
#     if success:
#         return value
#     return None


# def identity_function(value):
#     return value


# @st.cache_data(ttl=CACHE_TIME)
# def build_blocks_to_use(
#     chain: ChainData, start_block: int | None = None, end_block: int | None = None, approx_num_blocks_per_day: int = 6
# ) -> list[int]:
#     """Returns a block approx every 4 hours. by default between when autopool was first deployed to the current block"""
#     start_block = chain.block_autopool_first_deployed if start_block is None else start_block
#     end_block = chain.client.eth.block_number if end_block is None else end_block
#     blocks_hop = int(86400 / chain.approx_seconds_per_block) // approx_num_blocks_per_day
#     blocks = [b for b in range(start_block, end_block, blocks_hop)]
#     return blocks


# # not used, might want later
# class CallWithToString():
#     # not used
#     def __init__(self, call:Call):
#         self.call = call

#     def __str__(self) -> str:
#         # the string repersentation of the names and handling functions eg safe_normalize etc
#         returns_functions = [(name, inspect.getsource(handling_function)) for name, handling_function in self.call.returns ]
#         return str((self.call.target.lower(), self.call.data, self.call.function, returns_functions))

#     # def __lt__(self, other: "Call") -> bool:
#     #     """
#     Enables sorting by comparing target, function, and args.

#     The only thing that matter is a consistant ordering, the actual order does not matter
#     """
#     if not isinstance(other, SortableCall):
#         return TypeError(f'not comparing call objects {type(other)=}')
#     return (self.call.target, self.call.function, self.call.args) < (other.call.target, other.call.function, other.call.args)

# def __eq__(self, other) -> bool:
#     """
#     Checks equality based on target, function, and args.
#     """
#     if not isinstance(other, SortableCall):
#         return False
#     return (self.call.target, self.call.function, self.call.args) == (other.call.target, other.call.function, other.call.args)

