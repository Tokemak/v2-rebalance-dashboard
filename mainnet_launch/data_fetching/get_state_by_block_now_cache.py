import pandas as pd
from multicall import Multicall, Call
import streamlit as st

import nest_asyncio
import asyncio


from mainnet_launch.constants import CACHE_TIME, ChainData, TokemakAddress

# needed to run these functions in a jupyter notebook
nest_asyncio.apply()


import inspect

# # Define a function
# def greet(name):
#     return f"Hello, {name}!"

# # Get the function's source code
# source_code = inspect.getsource(greet)
# print("Source Code:\n", source_code)

# # Serialize the source code (as a string)
# serialized_source = source_code.encode('utf-8')

# # Deserialize and reconstruct the function
# deserialized_source = serialized_source.decode('utf-8')
# local_namespace = {}
# exec(deserialized_source, globals(), local_namespace)
# reconstructed_greet = local_namespace['greet']

# # Use the reconstructed function
# print(reconstructed_greet("Bob"))  # Output: Hello, Bob!


# pass


MULTICALL_V3 = TokemakAddress(
    eth="0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696", base="0xcA11bde05977b3631167028862bE2a173976CA11"
)


# def _save_data_into_muliticall_logs_db(
#     block:int, chain:ChainData, multicall:Multicall, mulitcall_response: list[any]
# ):


def _tester():

    import inspect

    from mainnet_launch.constants import ETH_CHAIN, AUTO_LRT, BASE_ETH, BASE_CHAIN, WETH

    def safe_normalize_with_bool_success(success: int, value: int):
        if success:
            return int(value) / 1e18
        return None

    block = 21238611

    weth_bal_of_autopool = Call(
        WETH(ETH_CHAIN),
        ["balanceOf(address)(uint256)", AUTO_LRT.autopool_eth_addr],
        [("AUTO_LRT_weth_bal", safe_normalize_with_bool_success)],
    )

    multicall = Multicall(calls=[weth_bal_of_autopool], block_id=block, _w3=ETH_CHAIN.client, require_success=False)

    response = multicall()

    pass


_tester()


async def safe_get_raw_state_by_block_one_block(calls: list[Call], block: int, chain: ChainData):
    # nice for testing
    multicall = Multicall(calls=calls, block_id=block, _w3=chain.client, require_success=False)
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
    chain: ChainData,
    semaphore_limits: int = (500, 200, 50, 20, 2),  # Increased limits
    include_block_number: bool = False,
) -> pd.DataFrame:
    return asyncio.run(async_safe_get_raw_state_by_block(calls, blocks, chain, semaphore_limits, include_block_number))


async def async_safe_get_raw_state_by_block(
    calls: list[Call],
    blocks: list[int],
    chain: ChainData,
    semaphore_limits: int = (500, 200, 50, 20, 2),  # Increased limits
    include_block_number: bool = False,
) -> pd.DataFrame:
    """
    Fetch a DataFame of each call in calls for each block in blocks fast
    """
    blocks_as_ints = [int(b) for b in blocks]
    # note only works after the multicall_v3 contract was deployed
    # block 12336033 (Apr-29-2021) on mainnet
    # block 5022 (Jun-15-2023) on Base
    # mostly a non issue but keep in mind that this only works on recent (within last 3 years) data

    get_block_call, get_timestamp_call = _build_default_block_and_timestamp_calls(chain)
    pending_multicalls = [
        Multicall(
            calls=[*calls, get_block_call, get_timestamp_call],
            block_id=int(block),
            _w3=chain.client,
            require_success=False,
        )
        for block in blocks_as_ints
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
        print(f"{len(blocks_as_ints)=} {blocks_as_ints[0]=} {blocks_as_ints[-1]=}")
        print(f"{calls=}")

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


@st.cache_data(ttl=CACHE_TIME)
def build_blocks_to_use(
    chain: ChainData, start_block: int | None = None, end_block: int | None = None, approx_num_blocks_per_day: int = 6
) -> list[int]:
    """Returns a block approx every 4 hours. by default between when autopool was first deployed to the current block"""
    start_block = chain.block_autopool_first_deployed if start_block is None else start_block
    end_block = chain.client.eth.block_number if end_block is None else end_block
    blocks_hop = int(86400 / chain.approx_seconds_per_block) // approx_num_blocks_per_day
    blocks = [b for b in range(start_block, end_block, blocks_hop)]
    return blocks