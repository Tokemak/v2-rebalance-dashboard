from dataclasses import dataclass

from multicall import Call
import streamlit as st

from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
)

from mainnet_launch.constants import CACHE_TIME, ALL_AUTOPOOLS, eth_client


@dataclass
class DestinationDetails:
    address: str
    symbol: str
    color: str


# this is so that the colors of each destination are consistent between plots
# TODO: this feature is not added yet
destination_colors = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
    "#ffbb78",
    "#98df8a",
    "#ff9896",
    "#c5b0d5",
    "#c49c94",
    "#f7b6d2",
    "#dbdb8d",
    "#9edae5",
    "#f4a442",
    "#c3e5f9",
    "#f0a3a1",
    "#b5e3e3",
    "#ffcc00",
    "#ff6600",
    "#ccff00",
    "#00ffcc",
    "#6600ff",
    "#ff00cc",
    "#ff9966",
    "#00ccff",
]


def _get_current_destinations_to_symbol(block: int) -> dict[str, str]:
    """Returns a dictionary of the current destinations: destination.symbol"""
    get_destinations_calls = [
        Call(a.autopool_eth_addr, "getDestinations()(address[])", [(f"{a.name} Idle", identity_with_bool_success)])
        for a in ALL_AUTOPOOLS
    ]
    destinations = get_state_by_one_block(get_destinations_calls, block)

    all_destinations = set([a.autopool_eth_addr for a in ALL_AUTOPOOLS])
    for _, destination_addresses in destinations.items():
        for d in destination_addresses:
            all_destinations.add(d)

    get_destination_symbols_calls = [
        Call(d, "symbol()(string)", [(eth_client.toChecksumAddress(d), identity_with_bool_success)])
        for d in all_destinations
    ]

    destination_to_symbol = get_state_by_one_block(get_destination_symbols_calls, block)

    for a in ALL_AUTOPOOLS:

        destination_to_symbol[a.autopool_eth_addr] = a.name + " idle"

    return destination_to_symbol


@st.cache_data(ttl=CACHE_TIME)  # 1 hours
def get_destination_details(block: int) -> dict[str, DestinationDetails]:
    destination_to_symbol = _get_current_destinations_to_symbol(block)

    destination_details = {}
    color_index = 0

    for address, symbol in destination_to_symbol.items():
        color = destination_colors[color_index % len(destination_colors)]
        destination_details[address] = DestinationDetails(address=address, symbol=symbol, color=color)
        destination_details[symbol] = DestinationDetails(address=address, symbol=symbol, color=color)
        color_index += 1

    return destination_details


destination_details = get_destination_details(eth_client.eth.block_number)


def attempt_destination_address_to_symbol(address: str) -> str:
    if address in destination_details:
        return destination_details[address].symbol
    else:
        return address


def attempt_destination_address_to_color(address: str) -> str:
    if address in destination_details:
        return destination_details[address].color
    else:
        return None


def attempt_destination_symbol_to_color(symbol: str) -> str:
    if symbol in destination_details:
        return destination_details[symbol].color
    else:
        return None


if __name__ == "__main__":

    for d in destination_details:
        print(d)
        print(destination_details[d])
