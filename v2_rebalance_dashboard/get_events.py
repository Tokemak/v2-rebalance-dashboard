import pandas as pd
import web3
from v2_rebalance_dashboard.get_state_by_block import eth_client


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
    event: web3.contract.ContractEvent, start_block: int, end_block: int, found_events: list
):
    """Recursively fetch all the `event` events between start_block and end_block"""
    try:
        event_filter = event.createFilter(fromBlock=start_block, toBlock=end_block)
        just_found_events = event_filter.get_all_entries()
        _flatten_events(just_found_events)
        found_events.extend(just_found_events)

    except ValueError as e:
        if e.args[0]["code"] != -32602:  # error code "Log response size exceeded" from Alchemy
            raise e

        mid = (start_block + end_block) // 2
        _recursive_helper_get_all_events_within_range(event, start_block, mid, found_events)
        _recursive_helper_get_all_events_within_range(event, mid + 1, end_block, found_events)


def events_to_df(found_events: list[web3.datastructures.AttributeDict]) -> pd.DataFrame:
    if len(found_events) == 0:
        return pd.DataFrame(columns=["block", "transaction_index", "log_index", "hash"])

    cleaned_events = []
    for event in found_events:
        cleaned_events.append(
            {
                **event["args"],
                "event": event["event"],
                "block": event["blockNumber"],
                "transaction_index": event["transactionIndex"],  # the position in the block
                "log_index": event["logIndex"],
                "hash": event["transactionHash"].hex(),
            }
        )
    return pd.DataFrame.from_records(cleaned_events).sort_values(["block", "log_index"])


def fetch_events(
    event: web3.contract.ContractEvent,
    start_block: int = 10091387,
    end_block: int = None,
) -> pd.DataFrame:
    """
    Collect every `event` between start_block and end_block into a DataFrame.

    start_block: defaults to 10091387, the earliest block with a timestamp in block collector. May 18, 2020

    include_timestamp: bool if you want to include the column timestamp
    """
    end_block = eth_client.eth.block_number if end_block is None else end_block

    found_events = list()
    _recursive_helper_get_all_events_within_range(event, start_block, end_block, found_events)
    event_df = events_to_df(found_events)
    return event_df


def get_each_event_in_contract(contract, end_block: int, include_timestamp: bool = False) -> dict[str, pd.DataFrame]:
    events_dict = dict()
    for e in contract.events:
        # add http fail retries?
        events_dict[e.event_name] = fetch_events(e, end_block=end_block, include_timestamp=include_timestamp)
    return events_dict
