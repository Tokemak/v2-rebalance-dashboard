"""

Post a message that says, what % ownership we have in total for each pool, and how large in base asset terms that pool is.

Aggregation is by pool instead of destination vault, because if we have multiple destination vaults for the same pool
"""

from mainnet_launch.pages.risk_metrics.percent_ownership_by_destination import (
    fetch_readable_our_tvl_by_destination,
)
from mainnet_launch.constants import *

from mainnet_launch.database.schema.full import *


from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_destinations_states_table import (
    build_lp_token_spot_and_safe_price_calls,
)

from mainnet_launch.database.postgres_operations import get_full_table_as_df

from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
)

from mainnet_launch.slack_messages.post_message import post_message_with_table


def _fetch_rich_tvl_by_destination(
    chain: ChainData, destinations: pd.DataFrame, autopool_destinations: pd.DataFrame
) -> pd.DataFrame:
    block = chain.get_block_near_top()
    our_tvl_by_destination_df = fetch_readable_our_tvl_by_destination(chain, block).copy()
    our_tvl_by_destination_df = pd.merge(
        our_tvl_by_destination_df,
        destinations[
            ["pool", "destination_vault_address", "underlying", "destination_vault_decimals", "underlying_symbol"]
        ],
        left_on="destination_vault_address",
        right_on="destination_vault_address",
        how="left",
    )
    our_tvl_by_destination_df["our_total_shares"] = our_tvl_by_destination_df.apply(
        lambda row: int(row["totalSupply"]) / 10 ** row["destination_vault_decimals"], axis=1
    )
    our_tvl_by_destination_df["total_shares"] = our_tvl_by_destination_df.apply(
        lambda row: int(row["underlyingTotalSupply"]) / 10 ** row["destination_vault_decimals"], axis=1
    )

    autopool_destinations["autopool"] = autopool_destinations["autopool_vault_address"].map(
        {a.autopool_eth_addr: a for a in ALL_AUTOPOOLS}
    )
    autopool_destinations["autopool_name"] = autopool_destinations["autopool_vault_address"].map(
        {a.autopool_eth_addr: a.name for a in ALL_AUTOPOOLS}
    )

    our_tvl_by_destination_df = pd.merge(
        our_tvl_by_destination_df,
        autopool_destinations[["destination_vault_address", "autopool", "autopool_name"]],
        left_on="destination_vault_address",
        right_on="destination_vault_address",
        how="left",
    )

    our_tvl_by_destination_df["base_asset_symbol"] = our_tvl_by_destination_df.apply(
        lambda row: row["autopool"].base_asset_symbol, axis=1
    )
    our_tvl_by_destination_df["chain_name"] = chain.name
    return our_tvl_by_destination_df


def _fetch_destination_safe_and_spot_prices_for_slack(our_tvl_by_destination_df: pd.DataFrame) -> pd.DataFrame:
    all_states = {}

    for autopool in ALL_AUTOPOOLS:

        this_autopool_df = our_tvl_by_destination_df[our_tvl_by_destination_df["autopool"] == autopool].copy()
        if this_autopool_df.empty:
            continue

        calls = build_lp_token_spot_and_safe_price_calls(
            destination_addresses=this_autopool_df["destination_vault_address"].tolist(),
            lp_token_addresses=this_autopool_df["underlying"].tolist(),
            pool_addresses=this_autopool_df["pool"].tolist(),
            autopool=autopool,
        )
        state = get_state_by_one_block(calls, autopool.chain.get_block_near_top(), autopool.chain)
        all_states.update(state)

    state_df = pd.DataFrame(all_states).T.rename(columns={0: "lp_token_spot_price", 1: "lp_token_safe_price"})
    state_df.index = state_df.index.get_level_values(0)
    state_df.reset_index(inplace=True)
    state_df = state_df.rename(columns={"index": "destination_vault_address"})
    return state_df


def fetch_destination_percent_ownership_with_sizes() -> pd.DataFrame:
    """Fetches live data, ~15 seconds"""
    all_readable_dfs = []
    destinations = get_full_table_as_df(Destinations)
    autopool_destinations = get_full_table_as_df(AutopoolDestinations)

    for chain in ALL_CHAINS:
        our_tvl_by_destination_df = _fetch_rich_tvl_by_destination(chain, destinations, autopool_destinations)
        state_df = _fetch_destination_safe_and_spot_prices_for_slack(our_tvl_by_destination_df)
        readable_df = pd.merge(
            our_tvl_by_destination_df,
            state_df,
            left_on="destination_vault_address",
            right_on="destination_vault_address",
            how="left",
        )

        readable_df["our_safe_tvl"] = (readable_df["our_total_shares"] * readable_df["lp_token_safe_price"]).round()
        readable_df["total_tvl"] = (readable_df["total_shares"] * readable_df["lp_token_safe_price"]).round()

        all_readable_dfs.append(readable_df)

    final_readable_df = pd.concat(all_readable_dfs, ignore_index=True)

    final_readable_df["total_tvl"] = final_readable_df.apply(
        lambda row: f"{row['total_tvl']:,.0f} {row['base_asset_symbol']}", axis=1
    )
    return final_readable_df


def post_ownership_exposure_message(percent_cutoff: float = 50.0):
    """Posts a table of the pools where we have > percent_cutoff % ownership, what autopools and"""
    readable_percent_ownership_by_pool = fetch_destination_percent_ownership_with_sizes()

    display_cols = [
        "underlying_symbol",
        "percent_ownership",
        "total_tvl",
        "destination_vault_address",
        "holding_autopools",
    ]

    dest_to_autopools = (
        readable_percent_ownership_by_pool.groupby("destination_vault_address")["autopool_name"].apply(tuple).to_dict()
    )
    readable_percent_ownership_by_pool["holding_autopools"] = readable_percent_ownership_by_pool[
        "destination_vault_address"
    ].map(dest_to_autopools)

    high_exposure_df = (
        readable_percent_ownership_by_pool[readable_percent_ownership_by_pool["percent_ownership"] > percent_cutoff][
            display_cols
        ]
        .sort_values(by="percent_ownership", ascending=False)
        .drop_duplicates()
    )

    high_exposure_df = high_exposure_df[["underlying_symbol", "percent_ownership", "total_tvl", "holding_autopools"]]

    high_exposure_df.rename(
        columns={
            "underlying_symbol": "Pool",
            "percent_ownership": "Ownership",
            "total_tvl": "Total TVL",
            "holding_autopools": "Autopools",
        },
        inplace=True,
    )
    high_exposure_df['Ownership'] = high_exposure_df['Ownership'].map(lambda x: f"{x:.2f}%")
    post_message_with_table(
        f"Tokemak Ownership by Pool\n Showing Pools with > {percent_cutoff}% Ownership".format(
            percent_cutoff=percent_cutoff
        ),
        high_exposure_df[["Pool", "Ownership", "Total TVL", "Autopools"]],
    )


if __name__ == "__main__":
    post_ownership_exposure_message(percent_cutoff=50.0)
