import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import pandas as pd
from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN, AUTO_LRT
from mainnet_launch.database.schema.full import (
    Blocks,
    DestinationStates,
    Destinations,
    AutopoolDestinations,
)
from mainnet_launch.database.schema.postgres_operations import (
    merge_tables_as_df,
    TableSelector,
)


def _fetch_destination_apr_data(autopool: AutopoolConstants) -> pd.DataFrame:
    destination_state_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                DestinationStates,
            ),
            TableSelector(
                Destinations,
                select_fields=[
                    Destinations.underlying_name,
                    Destinations.exchange_name,
                    Destinations.pool,
                ],
                join_on=(Destinations.destination_vault_address == DestinationStates.destination_vault_address)
                & (Destinations.chain_id == DestinationStates.chain_id),
            ),
            TableSelector(
                AutopoolDestinations,
                select_fields=[
                    AutopoolDestinations.autopool_vault_address,
                ],
                join_on=(AutopoolDestinations.destination_vault_address == DestinationStates.destination_vault_address)
                & (AutopoolDestinations.chain_id == DestinationStates.chain_id),
            ),
            TableSelector(
                Blocks,
                [Blocks.datetime],
                (DestinationStates.block == Blocks.block) & (DestinationStates.chain_id == Blocks.chain_id),
            ),
        ],
        where_clause=(AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr),
        order_by=Blocks.datetime,
    )

    destination_state_df = destination_state_df[destination_state_df["datetime"] >= autopool.start_display_date].copy()

    destination_state_df["readable_name"] = destination_state_df.apply(
        lambda row: f"{row['underlying_name']} ({row['exchange_name']})", axis=1
    )

    # this is to account the destination vaults getting upgraded
    destination_state_df = destination_state_df.groupby(["readable_name", "datetime"]).max().reset_index()
    return destination_state_df


def fetch_and_render_destination_apr_data(autopool: AutopoolConstants) -> go.Figure:
    destination_state_df = _fetch_destination_apr_data(autopool)

    destination_state_df["base_apr"] = pd.to_numeric(destination_state_df["base_apr"])
    destination_state_df["fee_plus_base_apr"] = pd.to_numeric(destination_state_df["fee_plus_base_apr"])
    destination_state_df["base_apr"] = destination_state_df["base_apr"].fillna(
        destination_state_df["fee_plus_base_apr"]
    )

    destination_choices = destination_state_df["readable_name"].unique()

    st.title("Destination APR Components")

    destination_choice = st.selectbox("Select a destination", destination_choices)

    if autopool == AUTO_LRT:
        apr_columns = ["incentive_apr", "fee_apr", "base_apr", "points_apr", "datetime"]
    elif autopool in ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN:
        apr_columns = ["incentive_apr", "base_apr", "datetime"]
    else:
        apr_columns = ["incentive_apr", "fee_apr", "base_apr", "datetime"]

    one_destination_df = (
        100
        * destination_state_df[destination_state_df["readable_name"] == destination_choice][apr_columns]
        .set_index("datetime")
        .astype(float)
        .resample("1D")
        .last()
    )

    apr_components_fig = px.line(one_destination_df, title=f"APR Components for {destination_choice}")
    _apply_default_style(apr_components_fig)

    st.plotly_chart(apr_components_fig, use_container_width=True)

    with st.expander("Destination Addresses"):

        all_time_destination_vault_addresses = destination_state_df[
            destination_state_df["readable_name"] == destination_choice
        ][["readable_name", "destination_vault_address", "pool"]].drop_duplicates()

        st.dataframe(all_time_destination_vault_addresses.T)

        # for dest in all_time_destination_vault_addresses:
        #     st.text(f"Destination Vault address poolool:  {dest}")

    apr_choice = st.selectbox("Pick a APR signal", apr_columns[:-1])
    summary_stats_df = (
        100
        * pd.pivot(destination_state_df, columns="readable_name", values=apr_choice, index="datetime")
        .resample("1D")
        .last()
    )

    st.plotly_chart(px.line(summary_stats_df, title=f"{autopool.name} {apr_choice}"), use_container_width=True)


def _apply_default_style(fig: go.Figure) -> None:
    fig.update_traces(line=dict(width=3))
    fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=600,
        width=600 * 3,
        font=dict(size=16),
        xaxis_title="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="lightgray"),
        yaxis=dict(showgrid=True, gridcolor="lightgray"),
        colorway=px.colors.qualitative.Set2,
    )


if __name__ == "__main__":
    from mainnet_launch.constants import *

    fetch_and_render_destination_apr_data(SONIC_USD)
