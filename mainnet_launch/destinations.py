from dataclasses import dataclass, asdict

from multicall import Call
import pandas as pd
from web3 import Web3

from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
    build_blocks_to_use,
)

from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, ChainData, ALL_CHAINS
from mainnet_launch.pages.autopool_diagnostics.lens_contract import fetch_pools_and_destinations_df
from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    get_earliest_block_from_table_with_chain,
    run_read_only_query,
    get_all_rows_in_table_by_autopool,
)
from mainnet_launch.database.should_update_database import should_update_table

DESTINATION_DETAILS_TABLE = "DESTINATION_DETAILS_TABLE"
CHAIN_BLOCK_QUERIED_TABLE = "CHAIN_BLOCK_QUERIED_TABLE"


@dataclass
class DestinationDetails:
    vaultAddress: str
    exchangeName: str

    dexPool: str
    lpTokenAddress: str
    lpTokenSymbol: str
    lpTokenName: str

    autopool: AutopoolConstants
    vault_name: str = None
    # TODO add base asset here

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

    def to_record(self) -> dict:
        return {
            "vaultAddress": self.vaultAddress,
            "exchangeName": self.exchangeName,
            "dexPool": self.dexPool,
            "lpTokenAddress": self.lpTokenAddress,
            "lpTokenSymbol": self.lpTokenSymbol,
            "lpTokenName": self.lpTokenName,
            "autopool": self.autopool.name,
            "vault_name": self.vault_name,
        }

    @classmethod
    def from_record(cls, record: dict):
        autopool_name_to_autopool = {a.name: a for a in ALL_AUTOPOOLS}

        autopool = autopool_name_to_autopool[record["autopool"]]

        return cls(
            vaultAddress=record["vaultAddress"],
            exchangeName=record["exchangeName"],
            dexPool=record["dexPool"],
            lpTokenAddress=record["lpTokenAddress"],
            lpTokenSymbol=record["lpTokenSymbol"],
            lpTokenName=record["lpTokenName"],
            autopool=autopool,
            vault_name=record["vault_name"],
        )


def add_new_destination_details_for_each_chain_to_table():
    if should_update_table(DESTINATION_DETAILS_TABLE):
        for chain in ALL_CHAINS:
            highest_block_already_fetched = get_earliest_block_from_table_with_chain(CHAIN_BLOCK_QUERIED_TABLE, chain)
            new_destination_details_df, new_highest_block = _fetch_destination_details_from_external_source(
                chain, highest_block_already_fetched
            )
            chain_block_table = pd.DataFrame.from_records([{"block": new_highest_block, "chain": chain.name}])
            write_dataframe_to_table(chain_block_table, CHAIN_BLOCK_QUERIED_TABLE)
            write_dataframe_to_table(new_destination_details_df, DESTINATION_DETAILS_TABLE)


def make_idle_destination_details(chain: ChainData) -> list[DestinationDetails]:
    # Idle is not included in pools and destinations
    idle_details = []

    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == chain:
            idle_details.append(
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


def _fetch_destination_details_from_external_source(
    chain: ChainData, highest_block_already_fetched: int
) -> pd.DataFrame:

    blocks = [b for b in build_blocks_to_use(chain) if b >= highest_block_already_fetched]
    # returns a list of all destinations along with their autopools even if the destinations have been replaced
    pools_and_destinations_df = fetch_pools_and_destinations_df(chain, blocks)
    autopool_pool_address_to_autopool = {a.autopool_eth_addr.lower(): a for a in ALL_AUTOPOOLS}

    if highest_block_already_fetched == chain.block_autopool_first_deployed:
        all_destination_details: list[DestinationDetails] = make_idle_destination_details(chain)
    else:
        all_destination_details = []

    def _add_to_all_destination_details(row):
        for on_chain_autopool_data, list_of_destinations in zip(
            row["getPoolsAndDestinations"]["autopools"], row["getPoolsAndDestinations"]["destinations"]
        ):
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
                    all_destination_details.append(destination_details)

    pools_and_destinations_df.apply(_add_to_all_destination_details, axis=1)

    unique_destination_vault_addressses = set([dest.vaultAddress for dest in all_destination_details])

    get_destination_names_calls = [
        Call(
            vaultAddress,
            "symbol()(string)",
            [(Web3.toChecksumAddress(vaultAddress), identity_with_bool_success)],
        )
        for vaultAddress in unique_destination_vault_addressses
    ]
    # the names don't change so we only need to get it once at the current highest block
    vault_addresses_to_names = get_state_by_one_block(get_destination_names_calls, block=max(blocks), chain=chain)

    for dest in all_destination_details:
        symbol = vault_addresses_to_names[Web3.toChecksumAddress(dest.vaultAddress)]
        symbol = symbol.replace("toke-WETH-", "")
        dest.vault_name = f"{symbol} ({dest.exchangeName})"

    destination_details_df = pd.DataFrame.from_records([dest.to_record() for dest in all_destination_details])

    destination_details_df = destination_details_df.drop_duplicates(keep="first")
    return destination_details_df, max(blocks)


def get_destination_details(autopool: AutopoolConstants) -> tuple[DestinationDetails]:
    add_new_destination_details_for_each_chain_to_table()
    destination_details_df = get_all_rows_in_table_by_autopool(DESTINATION_DETAILS_TABLE, autopool)

    destination_details = [DestinationDetails.from_record(r) for r in destination_details_df.to_records()]
    return destination_details


if __name__ == "__main__":
    from mainnet_launch.constants import BASE_CHAIN, ETH_CHAIN, BAL_ETH, AUTO_ETH

    details1 = get_destination_details(BAL_ETH)
    details2 = get_destination_details(AUTO_ETH)

    pass
