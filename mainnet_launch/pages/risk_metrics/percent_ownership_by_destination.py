import math


import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from web3 import Web3
from multicall import Call


from mainnet_launch.constants import ChainData, ALL_AUTOPOOLS, AutopoolConstants, AUTO_USD
from mainnet_launch.database.schema.full import Destinations, AutopoolDestinations
from mainnet_launch.database.schema.postgres_operations import get_full_table_as_df
from mainnet_launch.data_fetching.get_state_by_block import identity_with_bool_success, get_state_by_one_block
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.exit_liquidity.update_total_usd_exit_liqudity import (
    get_portion_ownership_by_pool,
)


def _fetch_readable_our_tvl_by_destination(chain: ChainData, block: int) -> pd.DataFrame:
    portion_ownership_df = (
        get_portion_ownership_by_pool(block, chain)
        .rename(columns={"index": "destination_vault_address", "getPool": "pool_address"})
        .copy()
    )
    portion_ownership_df["destination_vault_address"] = portion_ownership_df["destination_vault_address"].apply(
        lambda x: Web3.toChecksumAddress(x)
    )

    full_destinations_df = get_full_table_as_df(Destinations, where_clause=Destinations.chain_id == chain.chain_id)[
        [
            "destination_vault_address",
            "underlying_name",
            "underlying_symbol",
            "pool_type",
            "exchange_name",
        ]
    ].copy()

    # TODO: consider adding our USD size of position here
    full_destinations_df["destination_vault_address"] = full_destinations_df["destination_vault_address"].apply(
        lambda x: Web3.toChecksumAddress(x)
    )
    df = (
        pd.merge(
            portion_ownership_df,
            full_destinations_df,
            left_on="destination_vault_address",
            right_on="destination_vault_address",
            how="left",
        )
        .sort_values("percent_ownership", ascending=False)
        .reset_index(drop=True)
    )

    return df[
        [
            "underlying_name",
            "percent_ownership",
            "pool_type",
            "exchange_name",
            "destination_vault_address",
            "pool_address",
            "totalSupply",
            "underlyingTotalSupply",
        ]
    ]


def limit_to_this_autopool_destinations(
    our_tvl_by_destination_df: pd.DataFrame, autopool: AutopoolConstants, block: int
) -> pd.DataFrame:
    """
    Filter the DataFrame to only include destinations that are part of the specified autopool.
    """

    autopool_destinations = get_full_table_as_df(
        AutopoolDestinations, where_clause=AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr
    )

    our_tvl_by_destination_df = our_tvl_by_destination_df[
        our_tvl_by_destination_df["destination_vault_address"].isin(
            autopool_destinations["destination_vault_address"].values
        )
    ].copy()

    return our_tvl_by_destination_df


def _add_this_autopool_balance_of(
    autopool: AutopoolConstants, our_tvl_by_destination_df: pd.DataFrame, block: int
) -> pd.DataFrame:
    autopool_destinations = get_full_table_as_df(
        AutopoolDestinations, where_clause=AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr
    )
    balance_of_calls = [
        Call(
            destination_vault_address,
            ["balanceOf(address)(uint256)", autopool.autopool_eth_addr],
            [(destination_vault_address, identity_with_bool_success)],
        )
        for destination_vault_address in autopool_destinations["destination_vault_address"].values
    ]

    balance_of_dict = get_state_by_one_block(
        balance_of_calls,
        block=block,
        chain=autopool.chain,
    )
    our_tvl_by_destination_df[f"{autopool.name} Balance Of"] = our_tvl_by_destination_df[
        "destination_vault_address"
    ].map(balance_of_dict)
    our_tvl_by_destination_df[f"Tokemak Other Autopool Balance Of"] = our_tvl_by_destination_df.apply(
        lambda row: int(row["totalSupply"]) - int(row[f"{autopool.name} Balance Of"]), axis=1
    )

    our_tvl_by_destination_df[f"{autopool.name} Percent Ownership"] = our_tvl_by_destination_df.apply(
        lambda row: 100 * (int(row[f"{autopool.name} Balance Of"]) / int(row["underlyingTotalSupply"])), axis=1
    )
    our_tvl_by_destination_df[f"Tokemak Other Autopool Percent Ownership"] = our_tvl_by_destination_df.apply(
        lambda row: 100 * (int(row[f"Tokemak Other Autopool Balance Of"]) / int(row["underlyingTotalSupply"])), axis=1
    )
    our_tvl_by_destination_df["Not Tokemak Percent Ownership"] = (
        100
        - our_tvl_by_destination_df[f"Tokemak Other Autopool Percent Ownership"]
        - our_tvl_by_destination_df[f"{autopool.name} Percent Ownership"]
    )

    return our_tvl_by_destination_df


def _render_methodology():
    with st.expander("Methodology"):
        st.markdown(
            """
            Percent Ownership is calculated by:

            This Autopool's percent ownership = `100 * (destination_vault_address.balanceOf(autopool) / destination_vault_address.underlyingTotalSupply()`

            Other autopool's percent ownership = `100 * (destination_vault_address.totalSupply() - destination_vault_address.balanceOf(autopool)) / destination_vault_address.underlyingTotalSupply()`

            Not Tokemak Percent Ownership = `100 - this autopool's percent ownership - other autopool's percent ownership`

            At the latest block on the chain.

            """
        )


def fetch_and_render_our_percent_ownership_of_each_destination():
    st.subheader("Percent Ownership by Destination")

    autopool: AutopoolConstants = st.selectbox("Select Autopool", ALL_AUTOPOOLS, index=0, format_func=lambda x: x.name)

    with st.spinner(f"Fetching {autopool.name} Percent Ownership By Destination..."):
        block = autopool.chain.client.eth.block_number
        our_tvl_by_destination_df = _fetch_readable_our_tvl_by_destination(autopool.chain, block)
        our_tvl_by_destination_df = limit_to_this_autopool_destinations(our_tvl_by_destination_df, autopool, block)
        this_autopool_destinations_df = _add_this_autopool_balance_of(autopool, our_tvl_by_destination_df, block)

    _render_autopool_portion_ownership_as_pie_charts(this_autopool_destinations_df, autopool)

    with st.expander("Table of Percent Ownership by Destination"):
        st.dataframe(
            this_autopool_destinations_df[
                [
                    "underlying_name",
                    "exchange_name",
                    f"{autopool.name} Percent Ownership",
                    "Tokemak Other Autopool Percent Ownership",
                    "Not Tokemak Percent Ownership",
                    "destination_vault_address",
                    "pool_address",
                ]
            ],
            use_container_width=True,
        )


def _render_autopool_portion_ownership_as_pie_charts(
    this_autopool_destinations_df: pd.DataFrame, autopool: AutopoolConstants
):
    """
    Render a grid of Plotly pie charts (4 per row) showing percent ownership breakdown
    for a given autopool DataFrame.

    Parameters:
    - this_autopool_destinations_df: pd.DataFrame with columns
      f'{autopool_name} Percent Ownership',
      'Tokemak Other Autopool Percent Ownership',
      'Not Tokemak Percent Ownership',
      and 'underlying_name'.
    - autopool_name: name string used to build the first column label.
    """
    df = this_autopool_destinations_df.reset_index(drop=True)

    autopool_col = f"{autopool.name} Percent Ownership"
    other_col = "Tokemak Other Autopool Percent Ownership"
    not_col = "Not Tokemak Percent Ownership"

    cols = 3
    rows = math.ceil(len(df) / cols)

    fig = make_subplots(
        rows=rows,
        cols=cols,
        specs=[[{"type": "domain"}] * cols for _ in range(rows)],
        subplot_titles=list(df["underlying_name"]),
    )

    # Add pie charts
    for idx, row in df.iterrows():
        r = idx // cols + 1
        c = idx % cols + 1

        fig.add_trace(
            go.Pie(
                labels=[autopool_col, other_col, not_col],
                values=[row[autopool_col], row[other_col], row[not_col]],
                marker=dict(colors=["#636EFA", "#EF553B", "#00CC96"]),
                showlegend=False,
            ),
            row=r,
            col=c,
        )

    fig.update_layout(height=300 * rows, width=300 * cols, margin=dict(t=50, b=20, l=20, r=20))
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":

    fetch_and_render_our_percent_ownership_of_each_destination()
