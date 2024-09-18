from multicall import Call
import streamlit as st

from mainnet_launch.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
)

from mainnet_launch.constants import ALL_AUTOPOOLS


@st.cache_data(ttl=12 * 3600)
def get_current_destinations_to_symbol(block: int) -> dict[str, str]:
    """Returns a dictionary of the current destinations: destination.symbol"""
    get_destinations_calls = [
        Call(a.autopool_eth_addr, "getDestinations()(address[])", [(a.name, identity_with_bool_success)])
        for a in ALL_AUTOPOOLS
    ]
    destinations = get_state_by_one_block(get_destinations_calls, block)

    all_destinations = set([a.autopool_eth_addr for a in ALL_AUTOPOOLS])
    for k, destination_addresses in destinations.items():
        for d in destination_addresses:
            all_destinations.add(d)

    get_destination_symbols_calls = [
        Call(d, "symbol()(string)", [(d, identity_with_bool_success)]) for d in all_destinations
    ]

    destination_to_symbol = get_state_by_one_block(get_destination_symbols_calls, block)

    return destination_to_symbol
