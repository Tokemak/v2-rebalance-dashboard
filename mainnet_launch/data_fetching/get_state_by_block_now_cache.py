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
import sqlite3

from threading import Lock
from dataclasses import dataclass

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
            print("failed to load", multicall_hash)
            return None
        print("loaded", multicall_hash)
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
        print("saved", multicall_hash)


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
    return asyncio.run(async_safe_get_raw_state_by_block(calls, blocks, chain, semaphore_limits, include_block_number))


@dataclass
class MulticallResponse:
    mulitcall: Multicall
    db_hash: str
    response: dict = None


def _fetch_already_cached_responses(calls: list[Call], blocks: list[int], chain: ChainData) -> list[MulticallResponse]:

    get_block_call, get_timestamp_call = _build_default_block_and_timestamp_calls(chain)

    # Prepare Multicall objects for all blocks
    all_multicalls_to_fetch = [
        Multicall(
            calls=[*calls, get_block_call, get_timestamp_call],
            block_id=int(block),
            _w3=chain.client,
            require_success=False,
        )
        for block in blocks
    ]

    multicall_responses = [MulticallResponse(m, _multicall_to_db_hash(m), None) for m in all_multicalls_to_fetch]

    cached_hash_to_response = _batch_get_multicall_logs([m.db_hash for m in multicall_responses])


    # update the mulitcallResponses with the cached responses already in teh db
    for m in multicall_responses:
        if m.db_hash in cached_hash_to_response:
            m.response = cached_hash_to_response[m.db_hash]

    return multicall_responses


async def async_safe_get_raw_state_by_block(
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

    if chain == ETH_CHAIN:
        highest_finalized_block = ETH_CHAIN.client.eth.get_block("latest").number - (
            64 * 5
        )  # 5 epochs approx 32 minutes
    elif chain == BASE_CHAIN:
        highest_finalized_block = BASE_CHAIN.client.eth.get_block("latest").number - (300)  # approx 10 minutes
    else:
        raise ValueError(f"{chain} is not Base or Mainnet, add custom highest_finalized block")

    multicall_responses: list[MulticallResponse] = _fetch_already_cached_responses(calls, blocks, chain)
    db_hashes_already_cached = [m.db_hash for m in multicall_responses if m.response is not None]
    hash_to_response = {m.db_hash:m.response for m in multicall_responses}
    lock = Lock()
    for semaphore_limit in semaphore_limits:
        if any([m.response is None for m in multicall_responses]):
            # if we are missing any responses try and get them
            semaphore = asyncio.Semaphore(semaphore_limit)

            async def _fetch_data(multicall_response: MulticallResponse):
                async with semaphore:
                    try:
                        response = await multicall_response.multicall.coroutine()
                        if response is None:
                            pass
                            raise ValueError('response should not be None')
                        with lock:
                            hash_to_response[multicall_response.db_hash] = response
                            print('added response to lock')
                    except Exception as e:
                        print(type(e), e)
                        raise e
                        # if (e.args[0]["code"] == -32000) | (e.args[0]["code"] == 502): bad historical call, rate limited

            tasks = [_fetch_data(m) for m in multicall_responses if hash_to_response[m.db_hash] == None]
            await asyncio.gather(*tasks)

    # at this point multicall_responses should be full

    if any([m.response is None for m in multicall_responses]):
        raise ValueError("failed to fetch a response for a mulitcall becasue a multicallResponse.response is None")

    multicall_responses_to_cache = [m for m in multicall_responses if m.db_hash not in db_hashes_already_cached]
    _batch_insert_multicall_logs(multicall_responses_to_cache, highest_finalized_block)

    df = _responses_to_df(multicall_responses, include_block_number)
    if len(df) != len(blocks):
        raise ValueError(
            f"expected to have a row for unique block but got an unexpected number of rows: {df.shape=} {len(blocks)=}"
        )

    return df


def _responses_to_df(multicall_responses: list[MulticallResponse], include_block_number: bool) -> pd.DataFrame:
    responses = [m.response for m in multicall_responses]
    df = pd.DataFrame.from_records(responses)
    df.set_index("timestamp", inplace=True)
    df.index = pd.to_datetime(df.index, unit="s", utc=True)
    df.sort_index(inplace=True)
    df["block"] = df["block"].astype(int)
    if not include_block_number:
        df.drop(columns="block", inplace=True)
    return df


def _batch_insert_multicall_logs(multicallResponses: list[MulticallResponse], highest_finalized_block: int):
    """
    Batch insert multiple multicall logs into the cache.

    Parameters:
        multicall_hashes (list[str]): List of multicall hash strings.
        responses (list[dict]): Corresponding list of response dictionaries.
    """

    hashes_to_insert = [m.db_hash for m in multicallResponses if m.mulitcall.block_id < highest_finalized_block]
    responses_to_insert = [m.response for m in multicallResponses if m.mulitcall.block_id < highest_finalized_block]

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


def _batch_get_multicall_logs(multicall_hashes: list[str]) -> dict[str, dict | None]:
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

    # only valid jsons should be here so we should fail on trying to load one that does not work
    cached_hash_to_response = {row[0]: json.loads((row[1])) for row in rows}
    return cached_hash_to_response


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


@st.cache_data(ttl=CACHE_TIME)
def build_blocks_to_use(
    chain: ChainData, start_block: int | None = None, end_block: int | None = None, approx_num_blocks_per_day: int = 6
) -> list[int]:
    # just start from the start block, and go until you hit the end block instead of going backwards
    """Returns a block approx every 4 hours. by default between when autopool was first deployed to the current block"""
    start_block = chain.block_autopool_first_deployed if start_block is None else start_block

    if end_block is None:
        # these are approx but more than safe. can't use `finalized` because we are on a version of web3.py that
        # was before proof of stake and can't update because multicall.py requires an old version
        if chain == ETH_CHAIN:
            end_block = ETH_CHAIN.client.eth.get_block("latest").number - (64 * 5)
        elif chain == BASE_CHAIN:
            end_block = chain.client.eth.get_block("latest") - 300
        else:
            raise ValueError("chain is not Base or Eth mainnet", chain)

    blocks_hop = int(86400 / chain.approx_seconds_per_block) // approx_num_blocks_per_day
    blocks = [b for b in range(start_block, end_block, blocks_hop)]
    return blocks


from mainnet_launch.constants import ETH_CHAIN, AUTO_LRT, BASE_ETH, BASE_CHAIN, WETH, AUTO_ETH
import random


def _make_calls():
    def safe_normalize_with_bool_success(success: int, value: int):
        if success:
            return int(value) / 1e18
        return None

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


def _test_get_state_once():
    print(ETH_CHAIN.client.eth.chainId)
    calls = _make_calls()
    block = 21238611 + random.randint(0, 10000)

    state = get_state_by_one_block(calls, block, ETH_CHAIN)
    state = get_state_by_one_block(calls, block, ETH_CHAIN)


def _test_get_many_states():
    print(ETH_CHAIN.client.eth.chainId)
    calls = _make_calls()
    block = 21238611 + random.randint(0, 100)
    blocks = [21238611 + i for i in range(100)]

    df = get_raw_state_by_blocks(calls, blocks, ETH_CHAIN)

    print("df2")

    df = get_raw_state_by_blocks(calls, blocks, ETH_CHAIN)
    pass


if __name__ == "__main__":
    # _test_get_state_once()
    _test_get_many_states()
