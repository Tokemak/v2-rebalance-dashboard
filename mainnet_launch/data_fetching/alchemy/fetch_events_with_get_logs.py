from enum import Enum
import concurrent.futures
import requests
from web3 import Web3
from web3.contract import ContractEvent
from web3._utils.filters import construct_event_filter_params

from mainnet_launch.constants import ChainData, SONIC_CHAIN, PLASMA_CHAIN, ALL_CHAINS
import random
import time

# you prob want to use bloom filters like in
# https://github.com/agiletechvn/go-ethereum-code-analysis/blob/master/eth-bloombits-and-filter-analysis.md?utm_source=chatgpt.com


# PLASMA_CHAIN: 100k blocks per chunk due to stricter RPC limits
# SONIC_CHAIN: 2M blocks per chunk, handles moderate limits
# Other chains (e.g., Base, Mainnet): 100M blocks (DEFAULT_CHUNK_SIZE), minimal restrictions
# https://www.alchemy.com/docs/chains/ethereum/ethereum-api-endpoints/eth-get-logs

NUM_THREADS_FOR_PRE_SPLIT_FETCH = 2
DEFAULT_CHUNK_SIZE = 100_000_000

# only 10k ranges are garanteed to work, but we need to get more than that becaues there are millions of blocks
# so the 503 failures are retried at least once before splitting and trying again
# these are finger in the wind values

PRE_SPLIT_BLOCK_CHUNK_SIZE = {
    PLASMA_CHAIN: 100_000,
    SONIC_CHAIN: 2_000_000,
    **{chain: DEFAULT_CHUNK_SIZE for chain in ALL_CHAINS if chain not in [PLASMA_CHAIN, SONIC_CHAIN]},
}


class AchemyRequestStatus(Enum):
    SUCCESS = 1
    SPLIT_RANGE_AND_TRY_AGAIN = 2


class AlchemyError(Enum):
    LOG_RESPONSE_SIZE_EXCEEDED = -32602
    RESPONSE_TOO_BIG_ERROR = -32008
    SERVER_SIDE_ERROR = 503


class AlchemyFetchEventsError(Exception):
    pass


def _rpc_post(url: str, payload: dict) -> tuple[dict, AchemyRequestStatus]:
    headers = {"Content-Type": "application/json"}

    r = requests.post(url, json=payload, headers=headers, timeout=30)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        # I don't like the aesthetics of this
        if (r.status_code == AlchemyError.SERVER_SIDE_ERROR.value) and (("sonic" in url) or ("plasma" in url)):
            print("retry error,splitting half and trying again")
            return [], AchemyRequestStatus.SPLIT_RANGE_AND_TRY_AGAIN
        else:
            raise AlchemyFetchEventsError(f"Non-retryable HTTP error {e} for payload {payload=}")
    out = r.json()

    if "error" in out:
        error_code = out["error"]["code"]
        if error_code in [AlchemyError.RESPONSE_TOO_BIG_ERROR.value, AlchemyError.LOG_RESPONSE_SIZE_EXCEEDED.value]:
            return [], AchemyRequestStatus.SPLIT_RANGE_AND_TRY_AGAIN
        else:
            raise AlchemyFetchEventsError(f"Non-retryable Alchemy error {out['error']}")

    raw_logs = out["result"]
    return raw_logs, AchemyRequestStatus.SUCCESS


def _eth_getlogs_once(
    rpc_url: str,
    addresses: list[str],
    topics: list | None,
    from_block_hex: str,
    to_block_hex: str,
) -> tuple[list[dict], AchemyRequestStatus]:

    if not isinstance(addresses, list):
        raise AlchemyFetchEventsError(f"addresses must be a list of addresses, got {addresses}")

    params = {"address": addresses, "fromBlock": from_block_hex, "toBlock": to_block_hex}

    if topics is not None:
        params["topics"] = topics

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [params], 
    }


    max_retries = 3
    base_delay = 1  # seconds
    
    for attempt in range(max_retries + 1):
        raw_logs, status = _rpc_post(rpc_url, payload)
        
        if status != AchemyRequestStatus.SPLIT_RANGE_AND_TRY_AGAIN or attempt == max_retries:
            return raw_logs, status
        
        # Exponential backoff with jitter
        delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
        print(f"Retry attempt {attempt + 1}/{max_retries} after {delay:.2f}s delay")
        time.sleep(delay)
    
    return raw_logs, status


def _build_address_and_topics_for_event(
    event: ContractEvent,
    argument_filters: dict | None,
    from_block_hex: str,
    to_block_hex: str,
):
    event_abi = event._get_event_abi()
    _, filter_params = construct_event_filter_params(
        event_abi=event_abi,
        abi_codec=event.web3.codec,
        address=Web3.toChecksumAddress(event.address),
        argument_filters=argument_filters,
        fromBlock=from_block_hex,
        toBlock=to_block_hex,
    )
    return filter_params


def _recursive_make_web3_getLogs_call(
    event: ContractEvent,
    chain: ChainData,
    start_block: int,
    end_block: int,
    argument_filters: dict | None = None,
    global_raw_logs: list[dict] | None = None,
    addresses: list[str] | None = None,
) -> None:
    """
    Collect every `event` between start_block and end_block into a DataFrame.
    Recusivly splits the range into smaller chunks on timeout or large response issues.
    """
    if end_block <= start_block:
        # not certain here on if this is the desired behavior
        raise AlchemyFetchEventsError(f"{end_block:,} must be greater than {start_block:,}")

    filter_params = _build_address_and_topics_for_event(event, argument_filters, hex(start_block), hex(end_block))
    # del filter_params["address"] # we don't care about the address here, we are passing it in separately, just for clarity

    raw_logs, status = _eth_getlogs_once(
        rpc_url=chain.client.provider.endpoint_uri,
        addresses=addresses,
        topics=filter_params["topics"],
        from_block_hex=filter_params["fromBlock"],
        to_block_hex=filter_params["toBlock"],
    )

    if status == AchemyRequestStatus.SUCCESS:
        global_raw_logs.extend(raw_logs)
        return

    elif status == AchemyRequestStatus.SPLIT_RANGE_AND_TRY_AGAIN:
        if start_block == end_block:
            raise AlchemyFetchEventsError(
                f"Retryable failure when fetching logs for {event} where end block and start block are both {start_block:,}"
            )
        else:
            mid_block = (start_block + end_block) // 2
            _recursive_make_web3_getLogs_call(
                event, chain, start_block, mid_block, argument_filters, global_raw_logs, addresses=addresses
            )
            _recursive_make_web3_getLogs_call(
                event, chain, mid_block + 1, end_block, argument_filters, global_raw_logs, addresses=addresses
            )
            return


def _fetch_events_with_pre_split(
    event: ContractEvent,
    chain: ChainData,
    start_block: int,
    end_block: int,
    argument_filters: dict,
    split_size: int,
    addresses: list[str],
) -> bool:
    """we want to pre split the sonic large block, because it fails (sporadically at larger ranges)"""

    new_start_and_end_blocks = []

    for mid_point in range(start_block, end_block, split_size):
        new_start_and_end_blocks.append([mid_point, mid_point + split_size - 1])

    new_start_and_end_blocks[-1][1] = end_block

    def _fetch_chunk(chunk_start_block: int, chunk_end_block: int) -> list[dict]:
        local_raw_logs: list[dict] = []
        _recursive_make_web3_getLogs_call(
            event=event,
            chain=chain,
            start_block=chunk_start_block,
            end_block=chunk_end_block,
            argument_filters=argument_filters,
            global_raw_logs=local_raw_logs,
            addresses=addresses,
        )
        return local_raw_logs

    global_raw_logs: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS_FOR_PRE_SPLIT_FETCH) as thread_pool_executor:
        future_to_block_range = {
            thread_pool_executor.submit(_fetch_chunk, start_block, end_block): (start_block, end_block)
            for (start_block, end_block) in new_start_and_end_blocks
        }
        for future in concurrent.futures.as_completed(future_to_block_range):
            chunk_logs = future.result()
            if chunk_logs:
                global_raw_logs.extend(chunk_logs)

    return global_raw_logs


def fetch_raw_event_logs(
    event: ContractEvent,
    chain: ChainData,
    start_block: int | None = None,
    end_block: int | None = None,
    argument_filters: dict | None = None,
    addresses: list[str] | None = None,
) -> list[dict]:
    start_block = chain.block_autopool_first_deployed if start_block is None else start_block
    end_block = chain.get_block_near_top() if end_block is None else end_block
    split_size = PRE_SPLIT_BLOCK_CHUNK_SIZE[chain]
    raw_logs = _fetch_events_with_pre_split(
        event, chain, start_block, end_block, argument_filters, split_size, addresses
    )
    return raw_logs
