"""Makes external calls, don't run as part of regular tests."""

import pytest
from mainnet_launch.constants import WETH, ETH_CHAIN
from mainnet_launch.abis import ERC_20_ABI

from mainnet_launch.data_fetching.get_events import fetch_events


def test_fetch_events_recusive_split():
    pass


def test_fetch_events_no_events():
    pass


def test_fetch_recent_events_on_each_chain():
    # weth, - 1000 blocks on sonic, eth, base,
    # expect at least some transfers
    pass


def test_fail_if_block_after_highest_block():
    pass


def fetch_fail_on_end_block_less_than_start_block():

    contract = ETH_CHAIN.client.eth.contract(address=WETH(ETH_CHAIN), abi=ERC_20_ABI)

    transfer_df = fetch_events(
        event=contract.events.Transfer,
        chain=ETH_CHAIN,
        start_block=20_000_000 + 1,
        end_block=20_000_000,
    )
