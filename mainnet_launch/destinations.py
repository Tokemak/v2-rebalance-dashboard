from dataclasses import dataclass

from multicall import Call
import streamlit as st
import pandas as pd

from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
)

from mainnet_launch.constants import CACHE_TIME, ALL_AUTOPOOLS, eth_client
from mainnet_launch.lens_contract import fetch_pools_and_destinations_df


@dataclass()
class DestinationDetails:
    vaultAddress: str
    exchangeName: str

    dexPool: str
    lpTokenAddress: str
    lpTokenSymbol: str
    lpTokenName: str

    autopool_vault_address: str
    autopool_vault_symbol: str

    vault_name: str = None


def make_idle_destination_details() -> list[DestinationDetails]:
    idle_details = []

    for a in ALL_AUTOPOOLS:
        idle_details.append(
            DestinationDetails(
                vaultAddress=a.autopool_eth_addr,
                exchangeName="Tokemak",
                dexPool=None,
                lpTokenAddress=None,
                lpTokenSymbol=None,
                lpTokenName=None,
                autopool_vault_address=a.autopool_eth_addr,
                autopool_vault_symbol=f"{a.name} Idle",
            )
        )
    return idle_details


def flat_destinations_to_DestinationDetails(flat_destinations: list[dict]) -> list[DestinationDetails]:
    fields = {field.name for field in DestinationDetails.__dataclass_fields__.values()}
    filtered_data = [{k: v for k, v in json_data.items() if k in fields} for json_data in flat_destinations]
    return [DestinationDetails(**data) for data in filtered_data]


def build_all_destinationDetails(pools_and_destinations_df: pd.DataFrame) -> list[DestinationDetails]:
    all_destination_details = make_idle_destination_details()
    found_pairs = []

    def _add_to_all_destination_details(row: dict):
        autopools = row["autopools"]
        destinations = row["destinations"]
        for a, d in zip(autopools, destinations):
            for dest in d:
                dest["autopool_vault_address"] = a["poolAddress"]
                dest["autopool_vault_symbol"] = a["symbol"]

        flat_destinations = [d for des in destinations for d in des]

        destinations = flat_destinations_to_DestinationDetails(flat_destinations)
        for dest in destinations:
            if (dest.vaultAddress, dest.autopool_vault_address) not in found_pairs:
                found_pairs.append((dest.vaultAddress, dest.autopool_vault_address))
                all_destination_details.append(dest)

    pools_and_destinations_df["getPoolsAndDestinations"].apply(_add_to_all_destination_details)

    return all_destination_details


@st.cache_data(ttl=CACHE_TIME)
def get_destination_details() -> list[DestinationDetails]:
    pools_and_destinations_df = fetch_pools_and_destinations_df()
    destination_details = build_all_destinationDetails(pools_and_destinations_df)

    get_destination_names_calls = [
        Call(
            dest.vaultAddress,
            "name()(string)",
            [(eth_client.toChecksumAddress(dest.vaultAddress), identity_with_bool_success)],
        )
        for dest in destination_details
    ]

    vault_addresses_to_names = get_state_by_one_block(get_destination_names_calls, eth_client.eth.block_number)

    for dest in destination_details:
        dest.vault_name = vault_addresses_to_names[eth_client.toChecksumAddress(dest.vaultAddress)]

    return destination_details


def attempt_destination_address_to_vault_name(address: str) -> str:
    destination_details = get_destination_details()
    vault_address_to_name = {
        eth_client.toChecksumAddress(dest.vaultAddress): dest.vault_name for dest in destination_details
    }
    if eth_client.isChecksumAddress(address):
        checksumAddress = eth_client.toChecksumAddress(address)
        return vault_address_to_name[checksumAddress] if checksumAddress in vault_address_to_name else checksumAddress
    return address
