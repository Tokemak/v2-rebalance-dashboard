from dataclasses import dataclass, asdict

from multicall import Call
import streamlit as st
import pandas as pd
from web3 import Web3

from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
)

from mainnet_launch.constants import CACHE_TIME, ALL_AUTOPOOLS, AutopoolConstants, ChainData
from mainnet_launch.lens_contract import fetch_pools_and_destinations_df


@dataclass()
class DestinationDetails:
    vaultAddress: str
    exchangeName: str

    dexPool: str
    lpTokenAddress: str
    lpTokenSymbol: str
    lpTokenName: str

    autopool: AutopoolConstants
    vault_name: str = None

    def __str__(self):
        details = "Destination Details:\n"
        for field, value in asdict(self).items():
            details += f" {field}\t: {value}\n"
        return details

    def __repr__(self) -> str:
        return self.__str__()

    def __hash__(self):
        return hash(
            self.vaultAddress + self.lpTokenAddress,
        )

    def to_readable_name(self) -> str:
        return f"{self.vault_name} {self.exchangeName} {self.vaultAddress[:5]}"


def make_idle_destination_details(chain: ChainData) -> set[DestinationDetails]:
    # Idle is not included in pools and destinations
    idle_details = set()

    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == chain:
            idle_details.add(
                DestinationDetails(
                    vaultAddress=Web3.toChecksumAddress(autopool.autopool_eth_addr),
                    exchangeName="tokemak",
                    dexPool=None,
                    lpTokenAddress=Web3.toChecksumAddress(autopool.autopool_eth_addr),
                    lpTokenSymbol=None,
                    lpTokenName=None,
                    autopool=autopool,
                    vault_name=None,  # added later with an onchain call
                )
            )

    return idle_details


@st.cache_data(ttl=CACHE_TIME)
def get_destination_details(autopool: AutopoolConstants, blocks: list[int]) -> tuple[DestinationDetails]:
    # retuns a list of all destinations along with their autopools even if the destinations have been replaced
    pools_and_destinations_df = fetch_pools_and_destinations_df(autopool.chain, blocks)
    all_destination_details: set[DestinationDetails] = make_idle_destination_details(autopool.chain)

    autopool_pool_address_to_autopool = {
        a.autopool_eth_addr.lower(): a for a in ALL_AUTOPOOLS if a.chain == autopool.chain
    }

    def _add_to_all_destination_details(row: dict):

        for on_chain_autopool_data, list_of_destinations in zip(row["autopools"], row["destinations"]):

            autopool_constant = autopool_pool_address_to_autopool.get(on_chain_autopool_data["poolAddress"].lower())
            # skip autopools that don't have an AutopoolConstant setup
            # this is so that the app won't break when a new autopool is deployed
            if autopool_constant is not None:
                for destination in list_of_destinations:
                    destination_details = DestinationDetails(
                        vaultAddress=Web3.toChecksumAddress(destination["vaultAddress"]),
                        exchangeName=destination["exchangeName"],
                        dexPool=Web3.toChecksumAddress(destination["dexPool"]),
                        lpTokenAddress=Web3.toChecksumAddress(destination["lpTokenAddress"]),
                        lpTokenName=destination["lpTokenName"],
                        lpTokenSymbol=destination["lpTokenSymbol"],
                        autopool=autopool_constant,
                        vault_name=None,  # added later with an onchain call
                    )
                    # add any destinations ever created regardless of if they are currently active
                    all_destination_details.add(destination_details)

    pools_and_destinations_df["getPoolsAndDestinations"].apply(_add_to_all_destination_details)

    get_destination_names_calls = [
        Call(
            dest.vaultAddress,
            "symbol()(string)",
            [(Web3.toChecksumAddress(dest.vaultAddress), identity_with_bool_success)],
        )
        for dest in all_destination_details
    ]
    # the names don't change so we only need to get it once at the current highest block
    vault_addresses_to_names = get_state_by_one_block(
        get_destination_names_calls, block=max(blocks), chain=autopool.chain
    )

    for dest in all_destination_details:
        symbol = vault_addresses_to_names[Web3.toChecksumAddress(dest.vaultAddress)]
        symbol = symbol.replace("toke-WETH-", "")
        dest.vault_name = f"{symbol} ({dest.exchangeName})"

    destination_details = [dest for dest in all_destination_details if dest.autopool == autopool]
    return tuple(destination_details)


if __name__ == "__main__":
    from mainnet_launch.constants import BASE_CHAIN, ETH_CHAIN, BAL_ETH, BASE_ETH
    from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use

    blocks = build_blocks_to_use(ETH_CHAIN)
    pass

    # details = get_destination_details(BAL_ETH, build_blocks_to_use(ETH_CHAIN)[::6])

    # details = get_destination_details(BASE_ETH, build_blocks_to_use(BASE_CHAIN)[::6])
