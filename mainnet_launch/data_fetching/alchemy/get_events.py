import pandas as pd
from dataclasses import dataclass
import concurrent.futures

from web3.contract import Contract, ContractEvent

from mainnet_launch.data_fetching.alchemy.fetch_events_with_get_logs import fetch_raw_event_logs
from mainnet_launch.data_fetching.alchemy.process_raw_event_logs import decode_logs
from mainnet_launch.constants import ChainData, PLASMA_CHAIN


def fetch_events(
    event: ContractEvent,
    chain: ChainData,
    start_block: int = None,
    end_block: int = None,
    argument_filters: dict | None = None,
    addresses: list[str] | None = None,
) -> pd.DataFrame:
    """
    Fetch all the `event` events between start_block and end_block into a DataFrame.
    addresses: list of contract addresses to filter logs from (if None, fetch from all addresses)
    either use just the address in the evnet or (all the addreses in addresses)

    all addressese are assumed to be on the same chain as `chain`
    """
    if addresses is None:
        addresses = [event.address]

    raw_logs = fetch_raw_event_logs(
        event=event,
        chain=chain,
        start_block=start_block,
        end_block=end_block,
        argument_filters=argument_filters,
        addresses=addresses,
    )

    df = decode_logs(event, raw_logs)
    return df


def get_each_event_in_contract(
    contract: Contract, chain: ChainData, start_block: int = None, end_block: int = None
) -> dict[str, pd.DataFrame]:
    events_dict = dict()
    for event in contract.events:
        events_dict[event.event_name] = fetch_events(event, chain=chain, start_block=start_block, end_block=end_block)
    return events_dict


if __name__ == "__main__":
    from mainnet_launch.constants import ETH_CHAIN, WETH, profile_function
    from mainnet_launch.abis import ERC_20_ABI

    toke_contract = ETH_CHAIN.client.eth.contract("0x2e9d63788249371f1DFC918a52f8d799F4a38C94", abi=ERC_20_ABI)
    TOKE_transfers = fetch_events(toke_contract.events.Transfer, ETH_CHAIN)
    pass
