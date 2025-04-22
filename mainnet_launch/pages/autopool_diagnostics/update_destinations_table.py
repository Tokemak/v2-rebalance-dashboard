"""Make sure that the destination table is current"""

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func
import pandas as pd

from mainnet_launch.database.schema.full import Destinations, LastAutopoolUpdated, Session
from mainnet_launch.database.schema.postgres_operations import insert_avoid_conflicts

from mainnet_launch.constants import AutopoolConstants, ChainData, ALL_CHAINS, ALL_AUTOPOOLS

from mainnet_launch.pages.autopool_diagnostics.lens_contract import fetch_pools_and_destinations_df
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use


def fetch_all_destinations_for_autopool(autopool: AutopoolConstants) -> list[Destinations]:
    """
    Retrieves all destination rows associated with a specific autopool.

    :param autopool: AutopoolConstants enum to filter by.
    :return: List of Destinations ORM instances.
    """
    with Session.begin() as session:
        destinations = (
            session.execute(select(Destinations).where(Destinations.autopool == autopool.autopool_eth_addr))
            .scalars()
            .all()
        )
    return destinations


def make_idle_destination_details(chain: ChainData) -> list[Destinations]:
    # Idle is not included in pools and destinations
    idle_details = []

    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == chain:
            idle_details.append(
                Destinations(
                    destination_vault_address=autopool.autopool_eth_addr),
                    exchangeName="tokemak",

                    chain_id=autopool.chain.chain_id,
                    name=  autopool.symbol,

                    symbol = autopool.symbol
                    dexPool=None,
                    lpTokenAddress=Web3.toChecksumAddress(autopool.autopool_eth_addr),
                    lpTokenSymbol=autopool.symbol,
                    lpTokenName=None,
                    autopool=autopool,
                    vault_name=None,  # added later with an onchain call
                )
            )

    return idle_details


def _fetch_destination_details_from_external_source(
    chain: ChainData, highest_block_already_fetched: int
) -> pd.DataFrame:

    blocks = build_blocks_to_use(chain, start_block=highest_block_already_fetched)
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


# def add_new_destination_details_for_each_chain_to_table():
#     for chain in ALL_CHAINS:
#         highest_block_already_fetched = get_earliest_block_from_table_with_chain(CHAIN_BLOCK_QUERIED_TABLE, chain)
#         new_destination_details_df, new_highest_block = _fetch_destination_details_from_external_source(
#             chain, highest_block_already_fetched
#         )
#         chain_block_table = pd.DataFrame.from_records([{"block": new_highest_block, "chain": chain.name}])
#         write_dataframe_to_table(chain_block_table, CHAIN_BLOCK_QUERIED_TABLE)
#         write_dataframe_to_table(new_destination_details_df, DESTINATION_DETAILS_TABLE)
