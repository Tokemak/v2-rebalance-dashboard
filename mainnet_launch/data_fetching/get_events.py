import requests
import pandas as pd


from web3._utils.events import get_event_data
from web3.contract import Contract, ContractEvent
from web3 import Web3
from web3._utils.filters import construct_event_filter_params

from mainnet_launch.constants import ChainData


def _rpc_post(url: str, payload: dict) -> dict:
    headers = {"Content-Type": "application/json"}
    r = requests.post(url, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    out = r.json()
    if "error" in out:
        raise ValueError(out["error"])
    logs = out["result"]

    # required for inside of events.get_event_data
    # topics are expected to be bytes, not hexstr
    # eth_utils.event_abi_to_log_topic since that returns bytes
    for log in logs:
        log["topics"] = [bytes.fromhex(topic[2:]) for topic in log["topics"]]
    return logs


def _eth_getlogs_once(
    rpc_url: str,
    address: str | list[str] | None,
    topics: list | None,
    from_block_hex: str | None,
    to_block_hex: str | None,
) -> list[dict]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [
            {
                **({"address": address} if address else {}),
                **({"topics": topics} if topics else {}),
                **({"fromBlock": from_block_hex} if from_block_hex else {}),
                **({"toBlock": to_block_hex} if to_block_hex else {}),
            }
        ],
    }
    return _rpc_post(rpc_url, payload)


def _build_address_and_topics_for_event(
    event: ContractEvent,
    argument_filters: dict | None,
    from_block_hex: str | None,
    to_block_hex: str | None,
):
    event_abi = event._get_event_abi()
    data_filters_set, filter_params = construct_event_filter_params(
        event_abi=event_abi,
        abi_codec=event.web3.codec,
        address=Web3.toChecksumAddress(event.address),
        argument_filters=argument_filters,
        fromBlock=from_block_hex,
        toBlock=to_block_hex,
    )
    return data_filters_set, filter_params


def _recursive_getlogs(
    event: ContractEvent,
    rpc_url: str,
    start_block: int,
    end_block: int,
    argument_filters: dict | None,
    cleaned_events_list: list,
):

    return None


def _decode_logs(event: ContractEvent, logs: list[dict]) -> list[dict]:
    """Decode topics+data back to event dicts compatible with your _flatten_events."""

    decoded = []
    event_abi = event._get_event_abi()

    for log in logs:
        log_entry_data = get_event_data(event.web3.codec, event_abi, log)

        flattened_log_args = {}
        for arg_name, value in log_entry_data["args"].items():
            if isinstance(value, list):
                for position, value_in_position in enumerate(value):
                    flattened_log_args[f"{arg_name}_{position}"] = value_in_position
            else:
                flattened_log_args[arg_name] = value

        decoded.append(
            {
                "event": log_entry_data["event"],
                "blockNumber": (int(log_entry_data["blockNumber"], 16)),
                "transactionIndex": (int(log_entry_data["transactionIndex"], 16)),
                "logIndex": int(log_entry_data["logIndex"], 16),
                "transactionHash": log_entry_data["transactionHash"],
                **flattened_log_args,
            }
        )

    return decoded


def fetch_events(
    event: ContractEvent,
    chain: ChainData,
    start_block: int,
    end_block: int | None = None,
    argument_filters: dict | None = None,
) -> pd.DataFrame:
    """
    Collect every `event` between start_block and end_block into a DataFrame.
    """

    end_block = chain.get_block_near_top() if end_block is None else end_block

    if end_block <= start_block:
        # empty range: return typed empty frame
        event_field_names = [i["name"] for i in event._get_event_abi()["inputs"]]
        return pd.DataFrame(columns=[*event_field_names, "event", "block", "transaction_index", "log_index", "hash"])

    from_block_hex, to_block_hex = hex(int(start_block)), hex(int(end_block))

    data_filters_set, filter_params = _build_address_and_topics_for_event(
        event, argument_filters, from_block_hex, to_block_hex
    )
    # not certain what this data_filters_set is for

    logs = _eth_getlogs_once(
        rpc_url=chain.client.provider.endpoint_uri,
        address=filter_params["address"],
        topics=filter_params["topics"],
        from_block_hex=filter_params["fromBlock"],
        to_block_hex=filter_params["toBlock"],
    )
    decoded_logs = _decode_logs(event, logs)
    log_event_df = pd.DataFrame(decoded_logs)

    return log_event_df


if __name__ == "__main__":
    from mainnet_launch.constants import WETH, ETH_CHAIN
    from mainnet_launch.abis import ERC_20_ABI

    contract = ETH_CHAIN.client.eth.contract(address=WETH(ETH_CHAIN), abi=ERC_20_ABI)

    transfer_df = fetch_events(
        event=contract.events.Transfer,
        chain=ETH_CHAIN,
        start_block=10_000_000,
        end_block=11_000_000,
    )
    

    transfer_df = fetch_events(
        event=contract.events.Transfer,
        chain=ETH_CHAIN,
        start_block=23258223 - 10,
        end_block=23258223,
    )
