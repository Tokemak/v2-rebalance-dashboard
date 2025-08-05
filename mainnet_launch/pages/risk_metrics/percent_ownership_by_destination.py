import math
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import plotly.express as px
from web3 import Web3
from multicall import Call


from mainnet_launch.constants import *
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


def _fetch_autopool_percent_ownership_of_each_destination(
    autopools: list[AutopoolConstants],
    our_tvl_by_destination_df: pd.DataFrame,
    block: int,
):
    autopool_destinations = get_full_table_as_df(
        AutopoolDestinations,
        where_clause=AutopoolDestinations.autopool_vault_address.in_(a.autopool_eth_addr for a in autopools),
    )

    balance_of_calls = []

    for autopool in autopools:
        for destination_vault_address in autopool_destinations["destination_vault_address"].values:
            balance_of_calls.append(
                Call(
                    destination_vault_address,
                    ["balanceOf(address)(uint256)", autopool.autopool_eth_addr],
                    [((autopool.autopool_eth_addr, destination_vault_address), identity_with_bool_success)],
                )
            )

    balance_of_dict = get_state_by_one_block(
        balance_of_calls,
        block=block,
        chain=autopools[0].chain,
    )

    percent_cols = []

    for autopool in autopools:
        autopool_name = autopool.name
        our_tvl_by_destination_df[f"{autopool_name} Balance Of"] = our_tvl_by_destination_df[
            "destination_vault_address"
        ].map(
            lambda destination_vault_address: balance_of_dict.get(
                (autopool.autopool_eth_addr, destination_vault_address), 0
            )
        )

        our_tvl_by_destination_df[f"{autopool_name} Percent Ownership"] = our_tvl_by_destination_df.apply(
            lambda row: 100 * (int(row[f"{autopool_name} Balance Of"]) / int(row["underlyingTotalSupply"])), axis=1
        )
        percent_cols.append(f"{autopool_name} Percent Ownership")

    our_tvl_by_destination_df["Not Tokemak Percent Ownership"] = our_tvl_by_destination_df.apply(
        lambda row: 100
        * (int(row["underlyingTotalSupply"]) - int(row["totalSupply"]))
        / int(row["underlyingTotalSupply"]),
        axis=1,
    )
    percent_cols.append("Not Tokemak Percent Ownership")

    return our_tvl_by_destination_df, percent_cols


# todo this can be moved to a common file
def render_pick_chain_and_base_asset_dropdown() -> tuple[ChainData, TokemakAddress, list[AutopoolConstants]]:
    chain_base_asset_groups = {
        (ETH_CHAIN, WETH): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH),
        (ETH_CHAIN, USDC): (AUTO_USD,),
        (ETH_CHAIN, DOLA): (AUTO_DOLA,),
        (SONIC_CHAIN, USDC): (SONIC_USD,),
        (BASE_CHAIN, WETH): (BASE_ETH,),
        (BASE_CHAIN, USDC): (BASE_USD,),
    }

    options = list(chain_base_asset_groups.keys())
    chain, base_asset = st.selectbox(
        "Pick a Chain & Base Asset:", options, format_func=lambda k: f"{k[0].name} chain and {k[1].name}"
    )
    valid_autopools = chain_base_asset_groups[(chain, base_asset)]

    return chain, base_asset, valid_autopools


def _make_pie_chart_color_palette() -> dict[str, str]:
    palette = px.colors.qualitative.Plotly
    pie_chart_palette = {
        f"{autopool.name} Percent Ownership": color
        for autopool, color in zip(
            ALL_AUTOPOOLS,
            palette[1:],
        )
    }
    pie_chart_palette["Not Tokemak Percent Ownership"] = palette[0]
    return pie_chart_palette


def _render_percent_ownership_by_destination(this_autopool_destinations_df: pd.DataFrame, percent_cols: list[str]):
    df = this_autopool_destinations_df.reset_index(drop=True)
    # only show destinations where we have some ownership
    df = df[df[percent_cols[:-1]].gt(0).any(axis=1)].copy().reset_index(drop=True)

    cols = 3
    rows = math.ceil(len(df) / cols)

    fig = make_subplots(
        rows=rows,
        cols=cols,
        specs=[[{"type": "domain"}] * cols for _ in range(rows)],
        subplot_titles=list(df["underlying_name"]),
    )

    pie_chart_palette = _make_pie_chart_color_palette()

    for idx, row in df.iterrows():

        # drop any zero-value pie slices
        labels = []
        vals = []
        for label, value in zip(percent_cols, [row[percent_col] for percent_col in percent_cols]):
            if value > 0:
                labels.append(label)
                vals.append(value)

        r = (idx // cols) + 1
        c = (idx % cols) + 1

        slice_colors = [pie_chart_palette[label] for label in labels]

        fig.add_trace(
            go.Pie(
                labels=labels,
                values=vals,
                marker=dict(colors=slice_colors),
                textinfo="percent",
                hoverinfo="label",
                showlegend=False,
            ),
            row=r,
            col=c,
        )
    fig.update_layout(height=300 * rows, width=300 * cols, margin=dict(t=50, b=20, l=20, r=20))

    for ann in fig.layout.annotations:
        # ann.y is in normalized (0→1) coords; we’ll use a fixed pixel shift instead
        ann.yshift = 18
    st.plotly_chart(fig, use_container_width=True)


def fetch_and_render_our_percent_ownership_of_each_destination():
    st.subheader("Percent Ownership by Destination")

    chain, base_asset, valid_autopools = render_pick_chain_and_base_asset_dropdown()

    with st.spinner(f"Fetching {chain.name} {base_asset.name} Percent Ownership By Destination..."):
        block = chain.client.eth.block_number
        our_tvl_by_destination_df = _fetch_readable_our_tvl_by_destination(chain, block)
        this_autopool_destinations_df, percent_cols = _fetch_autopool_percent_ownership_of_each_destination(
            valid_autopools, our_tvl_by_destination_df, block
        )

    _render_percent_ownership_by_destination(this_autopool_destinations_df, percent_cols)

    # add a download button in one line
    st.download_button(
        label="Download Percent Ownership Data",
        data=this_autopool_destinations_df.to_csv(index=False),
        file_name=f"{chain.name}_{base_asset.name}_percent_ownership_by_destination.csv",
        mime="text/csv",
    )
    _render_methodology()


def _render_methodology():
    with st.expander("Readme"):
        st.markdown(
            """
            Percent Ownership is calculated by:
            This Autopool's Percent Ownership = `100 * (destination_vault_address.balanceOf(autopool) / destination_vault_address.underlyingTotalSupply()`
            Not Tokemak Percent Ownership = `100 - (100 * (destination_vault_address.totalSupply() / destination_vault_address.underlyingTotalSupply()))`
            At the latest block on the chain.
            """
        )


if __name__ == "__main__":
    fetch_and_render_our_percent_ownership_of_each_destination()

# remove 0 markers
# remove redundent info in pie charts
# switch to autopool -> color method
