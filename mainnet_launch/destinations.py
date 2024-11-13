from dataclasses import dataclass, asdict

from multicall import Call
import streamlit as st
import pandas as pd

from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
)

from mainnet_launch.constants import CACHE_TIME, ALL_AUTOPOOLS, eth_client, AutopoolConstants
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
            (
                self.vaultAddress,
                self.exchangeName,
                self.dexPool,
                self.lpTokenAddress,
                self.lpTokenSymbol,
                self.lpTokenName,
            )
        )

    def to_readable_name(self) -> str:
        return f"{self.vault_name} {self.exchangeName} {self.vaultAddress[:5]}"


def make_idle_destination_details() -> set[DestinationDetails]:
    # Idle is not included in pools and destinations
    idle_details = set()

    for autopool in ALL_AUTOPOOLS:
        idle_details.add(
            DestinationDetails(
                vaultAddress=eth_client.toChecksumAddress(autopool.autopool_eth_addr),
                exchangeName="tokemak",
                dexPool=None,
                lpTokenAddress=eth_client.toChecksumAddress(autopool.autopool_eth_addr),
                lpTokenSymbol=None,
                lpTokenName=None,
                autopool=autopool,
                vault_name=None,  # added later with an onchain call
            )
        )
    return idle_details


def autopool_data_to_autopool_constant(autopool: dict) -> AutopoolConstants:
    autopool_constant = [
        c
        for c in ALL_AUTOPOOLS
        if eth_client.toChecksumAddress(autopool["poolAddress"]) == eth_client.toChecksumAddress(c.autopool_eth_addr)
    ][0]
    return autopool_constant


@st.cache_data(ttl=CACHE_TIME)
def get_destination_details() -> list[DestinationDetails]:
    # retuns a list of all destinations along with their autopools even if the destinations have been replaced

    pools_and_destinations_df = fetch_pools_and_destinations_df()
    all_destination_details: set[DestinationDetails] = make_idle_destination_details()

    def _add_to_all_destination_details(row: dict):
        autopools = row["autopools"]

        if len(autopools) != 3:
            raise ValueError("Only expects 3 autopools, found not 3:", str(row))

        list_of_list_of_destinations = row["destinations"]

        autopool_constants = [autopool_data_to_autopool_constant(autopool) for autopool in autopools]

        for autopool_constant, list_of_destinations in zip(autopool_constants, list_of_list_of_destinations):

            for destination in list_of_destinations:
                destination_details = DestinationDetails(
                    vaultAddress=eth_client.toChecksumAddress(destination["vaultAddress"]),
                    exchangeName=destination["exchangeName"],
                    dexPool=eth_client.toChecksumAddress(destination["dexPool"]),
                    lpTokenAddress=eth_client.toChecksumAddress(destination["lpTokenAddress"]),
                    lpTokenName=destination["lpTokenName"],
                    lpTokenSymbol=destination["lpTokenSymbol"],
                    autopool=autopool_constant,
                    vault_name=None,  # added later with an onchain call
                )

                all_destination_details.add(destination_details)

    pools_and_destinations_df["getPoolsAndDestinations"].apply(_add_to_all_destination_details)

    get_destination_names_calls = [
        Call(
            dest.vaultAddress,
            "symbol()(string)",
            [(eth_client.toChecksumAddress(dest.vaultAddress), identity_with_bool_success)],
        )
        for dest in all_destination_details
    ]
    # the names don't change so we only need to get it once
    vault_addresses_to_names = get_state_by_one_block(get_destination_names_calls, eth_client.eth.block_number)

    for dest in all_destination_details:
        symbol = vault_addresses_to_names[eth_client.toChecksumAddress(dest.vaultAddress)]
        symbol = symbol.replace("toke-WETH-", "")
        dest.vault_name = f"{symbol} ({dest.exchangeName})"

    return list(all_destination_details)
