import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from mainnet_launch.constants import AutopoolConstants

from mainnet_launch.database.schema.full import (
    Blocks,
    DestinationStates,
    Destinations,
    AutopoolDestinationStates,
)
from mainnet_launch.database.postgres_operations import (
    merge_tables_as_df,
    TableSelector,
)


def fetch_and_render_weighted_crm_data(autopool: AutopoolConstants):
    composite_return_out_fig, composite_return_in_fig = _fetch_weighted_composite_return_df(autopool)

    st.plotly_chart(composite_return_out_fig, use_container_width=True)
    st.plotly_chart(composite_return_in_fig, use_container_width=True)

    with st.expander("See explanation"):
        st.write(
            f"""
            - Composite Return Out: `getDestinationSummaryStats()['compositeReturn']` for the destination with direction "out" and amount 0
            - Composite Return Out: `getDestinationSummaryStats()['compositeReturn']` for the destination with direction "out" and amount 0

             - {autopool.name} Weighted Expected Return. Weights is the portion of TVL in the destination values, are Composite Return Out / In
            """
        )


def _fetch_weighted_composite_return_df(autopool: AutopoolConstants) -> go.Figure:
    destination_state_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                DestinationStates,
                select_fields=[
                    DestinationStates.destination_vault_address,
                    DestinationStates.lp_token_safe_price,
                    DestinationStates.total_apr_out,
                    DestinationStates.total_apr_in,
                ],
            ),
            TableSelector(
                Destinations,
                select_fields=[
                    Destinations.underlying_name,
                    Destinations.exchange_name,
                ],
                join_on=(Destinations.destination_vault_address == DestinationStates.destination_vault_address)
                & (Destinations.chain_id == DestinationStates.chain_id),
            ),
            TableSelector(
                AutopoolDestinationStates,
                [
                    AutopoolDestinationStates.owned_shares,
                ],
                (DestinationStates.destination_vault_address == AutopoolDestinationStates.destination_vault_address)
                & (DestinationStates.chain_id == AutopoolDestinationStates.chain_id)
                & (DestinationStates.block == AutopoolDestinationStates.block),
            ),
            TableSelector(
                Blocks,
                [Blocks.datetime, Blocks.block],
                (DestinationStates.block == Blocks.block) & (DestinationStates.chain_id == Blocks.chain_id),
            ),
        ],
        where_clause=(AutopoolDestinationStates.autopool_vault_address == autopool.autopool_eth_addr)
        & (AutopoolDestinationStates.block > autopool.block_deployed),
        order_by=Blocks.datetime,
    )

    destination_state_df = destination_state_df[destination_state_df["datetime"] >= autopool.start_display_date].copy()

    destination_state_df["readable_destination_name"] = destination_state_df.apply(
        lambda row: f"{row['underlying_name']} ({row['exchange_name']})", axis=1
    )

    sum_df = (
        destination_state_df.groupby(["readable_destination_name", "datetime"])[["owned_shares"]].sum().reset_index()
    )

    owned_shares_df = (
        sum_df.pivot(index="datetime", values="owned_shares", columns="readable_destination_name").resample("1D").last()
    )

    max_df = (
        destination_state_df.groupby(["readable_destination_name", "datetime"])[
            ["lp_token_safe_price", "total_apr_out", "total_apr_in"]
        ]
        .max()
        .reset_index()
    )

    lp_token_safe_price_df = (
        max_df.pivot(index="datetime", values="lp_token_safe_price", columns="readable_destination_name")
        .resample("1D")
        .last()
    )

    allocation_df = (lp_token_safe_price_df * owned_shares_df).fillna(0)

    portion_allocation_df = allocation_df.div(allocation_df.sum(axis=1), axis=0)

    total_apr_out_df = 100 * (
        max_df.pivot(index="datetime", values="total_apr_out", columns="readable_destination_name")
        .resample("1D")
        .last()
    )
    total_apr_out_df[f"{autopool.name} CR"] = (total_apr_out_df * portion_allocation_df).sum(axis=1)

    total_apr_in_df = (
        100
        * max_df.pivot(index="datetime", values="total_apr_in", columns="readable_destination_name")
        .resample("1D")
        .last()
    )
    total_apr_in_df[f"{autopool.name} CR"] = (total_apr_in_df * portion_allocation_df).sum(axis=1)

    composite_return_out_fig = px.line(total_apr_out_df, title=f"{autopool.name} Composite Return Out")

    _apply_default_style(composite_return_out_fig)
    composite_return_out_fig.update_layout(yaxis_title="Composite Return Out (%)")
    composite_return_out_fig.update_traces(
        selector=dict(name=f"{autopool.name} CR"),
        line=dict(dash="dash", color="blue"),
    )

    composite_return_in_fig = px.line(total_apr_in_df, title=f"{autopool.name} Composite Return In")

    _apply_default_style(composite_return_in_fig)
    composite_return_in_fig.update_layout(yaxis_title="Composite Return In (%)")
    composite_return_in_fig.update_traces(
        selector=dict(name=f"{autopool.name} CR"),
        line=dict(dash="dash", color="blue"),
    )

    return composite_return_out_fig, composite_return_in_fig


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
    from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS
    from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use

    # fetch_and_render_weighted_crm_data(ALL_AUTOPOOLS[0])

    fetch_and_render_weighted_crm_data(ALL_AUTOPOOLS[-1])
