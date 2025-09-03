"""Makes external calls, don't run as part of regular tests."""

import pytest
from mainnet_launch.constants import *
from mainnet_launch.abis import ERC_20_ABI

from mainnet_launch.data_fetching.get_events import fetch_events


def test_fetch_events_recusive_split():
    pass


def test_fetch_events_no_events():
    pass


def fetch_each_chain():
    # weth, - 1000 blocks on sonic, eth, base,
    # expect at least some transfers

    for chain in ALL_CHAINS:

        contract = chain.client.eth.contract(address=WETH(chain), abi=ERC_20_ABI)

        transfer_df = fetch_events(
            event=contract.events.Transfer,
            chain=chain,
            start_block=chain.block_autopool_first_deployed,
            end_block=chain.block_autopool_first_deployed + 100,
        )
        print(transfer_df.shape, chain.name)
        # not sure why sonic works here, it doesn't work for autopool transfers


def test_fetch_transfers_on_each_autopool():
    for autopool in ALL_AUTOPOOLS:
        contract = autopool.chain.client.eth.contract(
            address=autopool.autopool_eth_addr,
            abi=ERC_20_ABI,
        )

        transfer_df = fetch_events(
            contract.events.Transfer,
            chain=autopool.chain,
            start_block=autopool.chain.get_block_near_top() - 3_000_000,
            end_block=autopool.chain.get_block_near_top(),
        )

        print(transfer_df.shape, autopool.name)


def test_fail_if_block_after_highest_block():
    pass


def fetch_fail_on_end_block_less_than_start_block():
    pass


if __name__ == "__main__":
    test_fetch_transfers_on_each_autopool()
