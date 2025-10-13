"""Note: getLogs supports the same event from multiple contracts in one call, but this is not implemented here."""

# don't add a silent parameter yet, shows where we are making redundent event fetches

from enum import Enum
import concurrent.futures
import requests
from web3 import Web3
from web3.contract import ContractEvent
from web3._utils.filters import construct_event_filter_params

from mainnet_launch.constants import ChainData, SONIC_CHAIN, PLASMA_CHAIN


PRE_SPLIT_BLOCK_CHUNK_SIZE = {PLASMA_CHAIN: 100_000, SONIC_CHAIN: 2_000_000}


class AchemyRequestStatus(Enum):
    SUCCESS = 1
    SPLIT_RANGE_AND_TRY_AGAIN = 2


class AlchemyError(Enum):
    LOG_RESPONSE_SIZE_EXCEEDED = -32602
    RESPONSE_TOO_BIG_ERROR = -32008
    SONIC_ONLY_ERROR = 503


class AlchemyFetchEventsError(Exception):
    pass


def _rpc_post(url: str, payload: dict) -> tuple[dict, AchemyRequestStatus]:
    headers = {"Content-Type": "application/json"}

    r = requests.post(url, json=payload, headers=headers, timeout=30)
    status = r.status_code
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        if (r.status_code == AlchemyError.SONIC_ONLY_ERROR.value) and r.url.startswith(
            "https://sonic-mainnet.g.alchemy.com"
        ):
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
    address: str | list[str] | None,
    topics: list | None,
    from_block: str,
    to_block: str,
) -> tuple[list[dict], AchemyRequestStatus]:

    params = {"address": address, "fromBlock": from_block, "toBlock": to_block}

    if topics is not None:
        params["topics"] = topics

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [params],
    }
    raw_logs, status = _rpc_post(rpc_url, payload)
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
) -> None:
    """
    Collect every `event` between start_block and end_block into a DataFrame.
    Recusivly splits the range into smaller chunks on timeout or large response issues.
    """
    if end_block <= start_block:
        # not certain here on if this is the desired behavior
        raise AlchemyFetchEventsError(f"{end_block:,} must be greater than {start_block:,}")

    filter_params = _build_address_and_topics_for_event(event, argument_filters, hex(start_block), hex(end_block))

    raw_logs, status = _eth_getlogs_once(
        rpc_url=chain.client.provider.endpoint_uri,
        address=filter_params["address"],
        topics=filter_params["topics"],
        from_block=filter_params["fromBlock"],
        to_block=filter_params["toBlock"],
    )

    if status == AchemyRequestStatus.SUCCESS:
        print(
            f"Fetched {len(raw_logs):,} logs for {event} from {start_block:,} to {end_block:,} ({end_block - start_block + 1:,} blocks)"
        )
        global_raw_logs.extend(raw_logs)
        return

    elif status == AchemyRequestStatus.SPLIT_RANGE_AND_TRY_AGAIN:
        if start_block == end_block:
            raise AlchemyFetchEventsError(
                f"Retryable failure when fetching logs for {event} where end block and start block are both {start_block:,}"
            )
        else:
            mid_block = (start_block + end_block) // 2
            print(
                f"Retryable failure when fetching logs for {event} on {chain.name=}"
                f"from {start_block:,} to {end_block:,} "
                f"({end_block - start_block + 1:,} blocks), splitting into:\n"
                f"  - {start_block:,} to {mid_block:,} ({mid_block - start_block + 1:,} blocks)\n"
                f"  - {mid_block + 1:,} to {end_block:,} ({end_block - mid_block:,} blocks)"
            )
            _recursive_make_web3_getLogs_call(event, chain, start_block, mid_block, argument_filters, global_raw_logs)
            _recursive_make_web3_getLogs_call(event, chain, mid_block + 1, end_block, argument_filters, global_raw_logs)
            return


def _fetch_events_with_pre_split(
    event: ContractEvent, chain: ChainData, start_block: int, end_block: int, argument_filters: dict, split_size: int
) -> bool:
    """we want to pre split the sonic large block, because it fails (sporadically at larger ranges)"""

    new_start_and_end_blocks = []

    for mid_point in range(start_block, end_block, split_size):
        new_start_and_end_blocks.append([mid_point, mid_point + split_size - 1])

    new_start_and_end_blocks[-1][1] = end_block

    def _fetch_chunk(chunk_start_block: int, chunk_end_block: int) -> list[dict]:
        local_raw_logs: list[dict] = []
        print(f"Fetching pre split {chain.name} chunk from {chunk_start_block:,} to {chunk_end_block:,}")
        _recursive_make_web3_getLogs_call(
            event=event,
            chain=chain,
            start_block=chunk_start_block,
            end_block=chunk_end_block,
            argument_filters=argument_filters,
            global_raw_logs=local_raw_logs,
        )
        return local_raw_logs

    global_raw_logs: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as thread_pool_executor:
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
    start_block: int = None,
    end_block: int | None = None,
    argument_filters: dict | None = None,
) -> list[dict]:
    start_block = chain.block_autopool_first_deployed if start_block is None else start_block
    end_block = chain.get_block_near_top() if end_block is None else end_block

    if chain in [PLASMA_CHAIN, SONIC_CHAIN]:
        raw_logs = _fetch_events_with_pre_split(
            event, chain, start_block, end_block, argument_filters, split_size=PRE_SPLIT_BLOCK_CHUNK_SIZE[chain]
        )
        return raw_logs

    raw_logs = []
    _recursive_make_web3_getLogs_call(event, chain, start_block, end_block, argument_filters, raw_logs)
    return raw_logs
