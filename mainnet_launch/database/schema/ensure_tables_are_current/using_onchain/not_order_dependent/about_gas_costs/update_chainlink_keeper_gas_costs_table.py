import pandas as pd

from mainnet_launch.abis import CHAINLINK_KEEPER_REGISTRY_ABI
from mainnet_launch.constants import ALL_AUTOPOOLS, ETH_CHAIN, ChainData

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_gas_costs.update_transactions_table_for_gas_costs import (
    fetch_tokemak_address_constants_dfs,
)
from mainnet_launch.database.postgres_operations import TableSelector, merge_tables_as_df, insert_avoid_conflicts
from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.database.schema.full import Transactions, Blocks, ChainlinkGasCosts
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)

# TODO convert to TokemakCostants

KEEPER_REGISTRY_CONTRACT_ADDRESS = "0x6593c7De001fC8542bB1703532EE1E5aA0D458fD"


def _fetch_topic_id_to_highest_already_fetched_block(chain: ChainData) -> dict[str, int]:
    # TODO this can be only in SQL
    prior_df = merge_tables_as_df(
        [
            TableSelector(ChainlinkGasCosts, select_fields=[ChainlinkGasCosts.chainlink_topic_id]),
            TableSelector(
                Transactions,
                select_fields=[Transactions.tx_hash, Transactions.block],
                join_on=ChainlinkGasCosts.tx_hash == Transactions.tx_hash,
            ),
            TableSelector(Blocks, select_fields=Blocks.datetime, join_on=Transactions.block == Blocks.block),
        ],
        where_clause=Blocks.chain_id == chain.chain_id,
    )
    topic_id_to_highest_already_fetched_block = prior_df.groupby("chainlink_topic_id")["block"].max().to_dict()
    return topic_id_to_highest_already_fetched_block


def _ensure_one_chain_chainlink_gas_costs_is_updated(
    chain: ChainData, chainlink_keepers_df: pd.DataFrame
) -> pd.DataFrame:

    topic_id_to_highest_already_fetched_block = _fetch_topic_id_to_highest_already_fetched_block(chain)

    keeper_network_topic_ids_to_name = (
        chainlink_keepers_df[chainlink_keepers_df["chain_id"] == chain.chain_id].set_index("id")["name"].to_dict()
    )
    upkeep_dfs = []

    contract = chain.client.eth.contract(KEEPER_REGISTRY_CONTRACT_ADDRESS, abi=CHAINLINK_KEEPER_REGISTRY_ABI)

    for topic_id, name in keeper_network_topic_ids_to_name.items():
        highest_block_already_fetched = topic_id_to_highest_already_fetched_block.get(topic_id, 0)
        # note still fetches for deprecated blocks, but it is fine
        our_upkeep_df = fetch_events(
            contract.events.UpkeepPerformed,
            chain=chain,
            start_block=highest_block_already_fetched,
            argument_filters={"id": int(topic_id)},
        )
        our_upkeep_df["name"] = name
        upkeep_dfs.append(our_upkeep_df)

    full_upkeep_df = pd.concat(upkeep_dfs, ignore_index=True)
    if full_upkeep_df.empty:
        # early exit if no new events
        return

    ensure_all_transactions_are_saved_in_db(full_upkeep_df["hash"].unique().tolist(), chain)

    new_chainlink_gas_costs_rows = full_upkeep_df.apply(
        lambda row: ChainlinkGasCosts(
            tx_hash=row["hash"],
            chainlink_topic_id=row["id"],
        ),
        axis=1,
    ).tolist()

    insert_avoid_conflicts(
        new_chainlink_gas_costs_rows,
        ChainlinkGasCosts,
    )


def ensure_chainlink_gas_costs_table_are_current() -> None:
    deployers_df, chainlink_keepers_df, service_accounts_df = fetch_tokemak_address_constants_dfs()
    for chain in [ETH_CHAIN]:
        _ensure_one_chain_chainlink_gas_costs_is_updated(chain, chainlink_keepers_df)


if __name__ == "__main__":

    deployers_df, chainlink_keepers_df, service_accounts_df = fetch_tokemak_address_constants_dfs()

    from mainnet_launch.constants import profile_function

    profile_function(_ensure_one_chain_chainlink_gas_costs_is_updated, ETH_CHAIN, chainlink_keepers_df)
