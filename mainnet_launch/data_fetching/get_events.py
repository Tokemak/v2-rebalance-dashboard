import pandas as pd
import web3
from requests.exceptions import ReadTimeout, HTTPError, ChunkedEncodingError
from web3.contract import Contract, ContractEvent
import time


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
    for event in just_found_events:
        updated_args = {}
        for arg_name, value in event["args"].items():
            if isinstance(value, list):
                for position, value_in_position in enumerate(value):
                    updated_args[f"{arg_name}_{position}"] = value_in_position
            else:
                updated_args[arg_name] = value
        # hack around the immutability
        event.__dict__["args"] = web3.datastructures.AttributeDict(updated_args)


def _recursive_helper_get_all_events_within_range(
    event: "web3.contract.ContractEvent",
    start_block: int,
    end_block: int,
    found_events: list,
    argument_filters: dict | None,
):
    """
    Recursively fetch all the `event` events between start_block and end_block.
    Immediately splits the range into smaller chunks on timeout or large response issues.

    TODO: consider usings eth.getLogs API calls like in

    https://web3py.readthedocs.io/en/stable/filters.html
    """
    try:
        # Try fetching events in the given range
        event_filter = event.createFilter(fromBlock=start_block, toBlock=end_block, argument_filters=argument_filters)
        just_found_events = event_filter.get_all_entries()
        _flatten_events(just_found_events)
        found_events.extend(just_found_events)

    except (TimeoutError, ValueError, ReadTimeout, HTTPError, ChunkedEncodingError) as e:
        if isinstance(e, ValueError) and e.args[0].get("code") != -32602:
            # Re-raise non "Log response size exceeded" errors
            raise e
        # otherwise cut the blocks in half and try again

        # Timeout or "Log response size exceeded" error - split the range
        mid = (start_block + end_block) // 2
        if start_block == mid or mid + 1 > end_block:
            # If the range is too small to split further, raise an exception
            raise RuntimeError(f"Unable to fetch events for blocks {start_block}-{end_block}")

        time.sleep(1)  # don't overwhelm api
        _recursive_helper_get_all_events_within_range(event, start_block, mid, found_events, argument_filters)
        _recursive_helper_get_all_events_within_range(event, mid + 1, end_block, found_events, argument_filters)


def events_to_df(found_events: list[web3.datastructures.AttributeDict]) -> pd.DataFrame:
    if len(found_events) == 0:
        return pd.DataFrame(columns=["block", "transaction_index", "log_index", "hash"])

    cleaned_events = []
    for event in found_events:
        cleaned_events.append(
            {
                **event["args"],
                "event": str(event["event"]),
                "block": int(event["blockNumber"]),
                "transaction_index": int(event["transactionIndex"]),
                "log_index": int(event["logIndex"]),
                "hash": str(event["transactionHash"].hex()).lower(),
            }
        )
    return pd.DataFrame.from_records(cleaned_events).sort_values(["block", "log_index"])


def fetch_events(
    event: ContractEvent,
    start_block: int = 15091387,
    end_block: int = None,
    argument_filters: dict | None = None,
) -> pd.DataFrame:
    """
    Collect every `event` between start_block and end_block into a DataFrame.
    """
    end_block = event.web3.eth.block_number if end_block is None else end_block
    found_events = []
    _recursive_helper_get_all_events_within_range(event, start_block, end_block, found_events, argument_filters)
    event_df = events_to_df(found_events)

    if len(event_df) == 0:
        # make sure that the df returned as the expected columns
        event_field_names = [i["name"] for i in event._get_event_abi()["inputs"]]
        event_df = pd.DataFrame(
            columns=[*event_field_names, str(event), "block", "transaction_index", "log_index", "hash"]
        )
        pass

    return event_df


def get_each_event_in_contract(contract: Contract, end_block: int = None) -> dict[str, pd.DataFrame]:
    end_block = contract.web3.eth.block_number if end_block is None else end_block
    events_dict = dict()
    for event in contract.events:
        # add http fail retries?
        events_dict[event.event_name] = fetch_events(event, end_block=end_block)
    return events_dict
