import pandas as pd
from dataclasses import dataclass
import concurrent.futures

from web3.contract import Contract, ContractEvent

from mainnet_launch.data_fetching.alchemy.fetch_events_with_get_logs import fetch_raw_event_logs
from mainnet_launch.data_fetching.alchemy.process_raw_event_logs import decode_logs
from mainnet_launch.constants import ChainData, PLASMA_CHAIN


class FetchEventsError(Exception):
    pass


def fetch_events(
    event: ContractEvent,
    chain: ChainData,
    start_block: int = None,
    end_block: int = None,
    argument_filters: dict | None = None,
) -> pd.DataFrame:
    """
    Fetch all the `event` events between start_block and end_block into a DataFrame.
    """
    raw_logs = fetch_raw_event_logs(
        event=event,
        chain=chain,
        start_block=start_block,
        end_block=end_block,
        argument_filters=argument_filters,
    )

    df = decode_logs(event, raw_logs)
    return df


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

    chain = events[0].chain
    if any(e.chain != chain for e in events):
        raise FetchEventsError("fetch_many_events requires all events to be on the same chain")

    if chain == PLASMA_CHAIN:
        num_threads = 1

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
        results = {}
        for future in concurrent.futures.as_completed(future_to_id):
            id_key = future_to_id[future]
            results[id_key] = future.result()

    return results


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

    weth_contract = ETH_CHAIN.client.eth.contract(address=WETH(ETH_CHAIN), abi=ERC_20_ABI)
    df = fetch_events(
        event=weth_contract.events.Transfer,
        chain=ETH_CHAIN,
        start_block=20_000_000,
        end_block=20_000_100,
    )
    pass
