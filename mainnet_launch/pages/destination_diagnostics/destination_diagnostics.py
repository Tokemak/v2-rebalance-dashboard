import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats


def fetch_and_render_destination_apr_data(autopool: AutopoolConstants):
    apr_components_fig = _make_apr_components_fig(autopool)
    st.plotly_chart(apr_components_fig, use_container_width=True)

    with st.expander("See explanation"):
        st.write(
            f"""
              APR Components: Show Unweighted Base, Fee, Incentive and Price Return of the Destination
            """
        )


def _make_apr_components_fig(autopool: AutopoolConstants) -> go.Figure:
    priceReturn_df = 100 * fetch_destination_summary_stats(autopool, "priceReturn")
    baseApr_df = 100 * fetch_destination_summary_stats(autopool, "baseApr")
    feeApr_df = 100 * fetch_destination_summary_stats(autopool, "feeApr")
    incentiveApr_df = 100 * fetch_destination_summary_stats(autopool, "incentiveApr")
    pointsApr_df = 100 * fetch_destination_summary_stats(autopool, "pointsApr")

    st.title("Destination APR Components")

    destination = st.selectbox("Select a destination", pointsApr_df.columns)

    plot_data = pd.DataFrame(
        {
            "Price Return": priceReturn_df[destination],
            "Base APR": baseApr_df[destination],
            "Incentive APR": incentiveApr_df[destination],
            "Fee APR": feeApr_df[destination],
            "Points APR": pointsApr_df[destination],
        },
        index=pointsApr_df.index,
    )

    apr_components_fig = px.line(plot_data, title=f"APR Components for {destination}")
    _apply_default_style(apr_components_fig)
    return apr_components_fig


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
