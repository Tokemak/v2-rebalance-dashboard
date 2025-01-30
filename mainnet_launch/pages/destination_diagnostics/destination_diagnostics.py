import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import (
    fetch_destination_summary_stats,
    get_destination_details,
)


def fetch_and_render_destination_apr_data(autopool: AutopoolConstants) -> go.Figure:
    priceReturn_df = 100 * fetch_destination_summary_stats(autopool, "priceReturn")
    baseApr_df = 100 * fetch_destination_summary_stats(autopool, "baseApr")
    feeApr_df = 100 * fetch_destination_summary_stats(autopool, "feeApr")
    incentiveApr_df = 100 * fetch_destination_summary_stats(autopool, "incentiveApr")
    pointsApr_df = 100 * fetch_destination_summary_stats(autopool, "pointsApr")

    st.title("Destination APR Components")

    destination_choice = st.selectbox("Select a destination", pointsApr_df.columns)

    plot_data = pd.DataFrame(
        {
            "Price Return": priceReturn_df[destination_choice],
            "Base APR": baseApr_df[destination_choice],
            "Incentive APR": incentiveApr_df[destination_choice],
            "Fee APR": feeApr_df[destination_choice],
            "Points APR": pointsApr_df[destination_choice],
        },
        index=pointsApr_df.index,
    )
    the_destinations = [d for d in get_destination_details(autopool) if d.vault_name == destination_choice]

    apr_components_fig = px.line(plot_data, title=f"APR Components for {destination_choice}")
    _apply_default_style(apr_components_fig)

    st.plotly_chart(apr_components_fig, use_container_width=True)
    st.write("Shows Unweighted Base, Fee, Incentive and Price Return of each Destination")

    with st.expander("Destination Addresses"):
        st.text(f"{the_destinations[0].vault_name}")
        for dest in the_destinations:
            st.text(f"{dest.vaultAddress=} {dest.dexPool=} {dest.lpTokenAddress=}")


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
