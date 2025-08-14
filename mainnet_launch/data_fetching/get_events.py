from dataclasses import dataclass
import concurrent.futures


from requests.exceptions import ReadTimeout, HTTPError, ChunkedEncodingError, ConnectionError
import pandas as pd
import web3
from web3.contract import Contract, ContractEvent

from mainnet_launch.constants import ChainData
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use


def _flatten_events(just_found_events: list[dict]) -> None:
    """
    If an emitted event has list args rename them to arg_name_1 ... arg_name_2

    This is because set and pandas don't work well with lists
    eg:
    AttributeDict(
        {'provider': '0x8a9664EeB6d595cd5d6dbD7B44A4Fb19d99b2089',
        'token_amounts': [156100153124040549569840, 4707055561829168550638],
        'token_supply': 398334677422224108708620})

    becomes

    AttributeDict(
        {'provider': '0x8a9664EeB6d595cd5d6dbD7B44A4Fb19d99b2089',
        'token_amounts_0': 156100153124040549569840,
        'token_amounts_1': 4707055561829168550638,
        'token_supply': 398334677422224108708620}
        )
    """
    cleaned_events = []
    for raw_event_data in just_found_events:
        event_data = {
            "event": str(raw_event_data["event"]),
            "block": int(raw_event_data["blockNumber"]),
            "transaction_index": int(raw_event_data["transactionIndex"]),
            "log_index": int(raw_event_data["logIndex"]),
            "hash": str(raw_event_data["transactionHash"].hex()).lower(),
        }
        updated_args = {}
        for arg_name, value in raw_event_data["args"].items():
            if isinstance(value, list):
                for position, value_in_position in enumerate(value):
                    updated_args[f"{arg_name}_{position}"] = value_in_position
            else:
                updated_args[arg_name] = value

        event_data.update(updated_args)
        cleaned_events.append(event_data)

    return cleaned_events


def _recursive_helper_get_all_events_within_range(
    event: "web3.contract.ContractEvent",
    start_block: int,
    end_block: int,
    clean_found_events: list,
    argument_filters: dict | None,
):
    """
    Recursively fetch all the `event` events between start_block and end_block.
    Immediately splits the range into smaller chunks on timeout or large response issues.
    # is sequential and recursive, generally fast enough, but not insanely fast
    """
    try:
        # Try fetching events in the given range
        try:
            event_filter = event.createFilter(
                fromBlock=start_block, toBlock=end_block, argument_filters=argument_filters
            )
        except Exception as e:
            raise e
        just_found_events = event_filter.get_all_entries()
        cleaned_events = _flatten_events(just_found_events)
        clean_found_events.extend(cleaned_events)

    except (TimeoutError, ValueError, ReadTimeout, HTTPError, ChunkedEncodingError, ConnectionError) as e:
        if (e.args[0].get("code") == 32000) and (e.args[0].get("message") == "filter not found"):
            # for some fast chains (at least sonic), but maybe others, asking alchemy for block to near the head
            # raises a error
            # so try again with a smaller top block
            new_end_block = end_block - 500
            _recursive_helper_get_all_events_within_range(
                event, start_block, new_end_block, clean_found_events, argument_filters
            )
            return

        elif isinstance(e, ValueError) and e.args[0].get("code") != -32602:
            print(f"{start_block=}, {end_block=}, {event=}, {e=}, {type(e)=}")
            raise e

        elif e.args[0].get("code") == -32602:
            # otherwise cut the blocks in half and try again
            # Timeout or "Log response size exceeded" error - split the range
            mid = (start_block + end_block) // 2
            if start_block == mid or mid + 1 > end_block:
                # If the range is too small to split further, raise an exception
                raise RuntimeError(f"Unable to fetch events for blocks {start_block}-{end_block}")

            _recursive_helper_get_all_events_within_range(event, start_block, mid, clean_found_events, argument_filters)
            _recursive_helper_get_all_events_within_range(
                event, mid + 1, end_block, clean_found_events, argument_filters
            )


def events_to_df(clean_found_events: list[dict]) -> pd.DataFrame:
    if len(clean_found_events) == 0:
        return pd.DataFrame(columns=["block", "transaction_index", "log_index", "hash"])

    return pd.DataFrame.from_records(clean_found_events).sort_values(["block", "log_index"])


def fetch_events(
    event: ContractEvent,
    chain: ChainData,
    start_block: int = 15091387,  # I don't like this start block
    end_block: int = None,
    argument_filters: dict | None = None,
) -> pd.DataFrame:
    """
    Collect every `event` between start_block and end_block into a DataFrame.
    """
    # -100 is to make sure that the block is properly indexed for alchemy
    # not totally certain this is the fix
    # be we get this error when using the current block for base

    # after 32222436 exists
    # 32222436 <class 'web3._utils.datatypes.DestinationVaultAdded'>
    # {'code': -32000, 'message': 'One of the blocks specified in filter (fromBlock, toBlock or blockHash) cannot be found.'} <class 'ValueError'>
    # this gives all the events up to -100 blocks for the current
    end_block = (chain.client.eth.block_number - 100) if end_block is None else end_block

    if end_block > start_block:
        clean_found_events = []
        _recursive_helper_get_all_events_within_range(
            event, int(start_block), int(end_block), clean_found_events, argument_filters
        )
        event_df = events_to_df(clean_found_events)
    else:
        event_df = pd.DataFrame()

    if len(event_df) == 0:
        # make sure that the df returned has the expected columns
        event_field_names = [i["name"] for i in event._get_event_abi()["inputs"]]
        event_df = pd.DataFrame(
            columns=[*event_field_names, "event", "block", "transaction_index", "log_index", "hash"]
        )
        pass

    return event_df


@dataclass
class FetchEventParams:
    event: ContractEvent
    chain: ChainData
    id: str
    start_block: int = None
    end_block: int = None
    argument_filters: dict = None


def fetch_many_events(events: list[FetchEventParams], num_threads: int = 16) -> dict[str, pd.DataFrame]:
    """Fetch many events concurrently"""
    results = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Map each submitted task to its index in the events list.
        future_to_id = {
            executor.submit(
                fetch_events,
                event=ep.event,
                chain=ep.chain,
                start_block=ep.start_block,
                end_block=ep.end_block,
                argument_filters=ep.argument_filters,
            ): ep.id
            for ep in events
        }
        # Process each future as it completes.
        for future in concurrent.futures.as_completed(future_to_id):
            # fail on any error in fetch_events
            id_key = future_to_id[future]
            results[id_key] = future.result()

    return results


def get_each_event_in_contract(
    contract: Contract, chain: ChainData, start_block: int = None, end_block: int = None
) -> dict[str, pd.DataFrame]:
    events_dict = dict()
    for event in contract.events:
        # add http fail retries?
        events_dict[event.event_name] = fetch_events(event, chain=chain, start_block=start_block, end_block=end_block)
    return events_dict
