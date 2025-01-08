import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats


def fetch_and_render_weighted_crm_data(autopool: AutopoolConstants):
    composite_return_fig = _make_all_destination_composite_return_df(autopool)
    st.plotly_chart(composite_return_fig, use_container_width=True)

    with st.expander("See explanation"):
        st.write(
            f"""
              Composite Return Out: Composite Return out of the various destinations
              {autopool.name} Weighted Expected Return: sum(% of TVL in Destination * Destination Composite Return)
            """
        )


def fetch_and_render_destination_apr_data(autopool: AutopoolConstants):
    apr_components_fig = _make_apr_components_fig(autopool)
    st.plotly_chart(apr_components_fig, use_container_width=True)

    with st.expander("See explanation"):
        st.write(
            f"""
              APR Components: Show Unweighted Base, Fee, Incentive and Price Return of the Destination
            """
        )


def _make_all_destination_composite_return_df(autopool: AutopoolConstants) -> go.Figure:
    compositeReturn_out_df = 100 * fetch_destination_summary_stats(autopool, "compositeReturn")
    pricePerShare_df = 100 * fetch_destination_summary_stats(autopool, "pricePerShare")
    ownedShares_df = 100 * fetch_destination_summary_stats(autopool, "ownedShares")
    allocation_df = pricePerShare_df * ownedShares_df
    portion_allocation_df = allocation_df.div(allocation_df.sum(axis=1), axis=0)
    autopool_weighted_expected_return = (compositeReturn_out_df * portion_allocation_df).sum(axis=1)
    compositeReturn_out_df[f"{autopool.name} CR"] = autopool_weighted_expected_return

    composite_return_fig = px.line(compositeReturn_out_df, title=f"{autopool.name} Destinations and composite Return")
    _apply_default_style(composite_return_fig)
    composite_return_fig.update_layout(yaxis_title="Composite Return (%)")
    composite_return_fig.update_traces(
        selector=dict(name=f"{autopool.name} CR"),
        line=dict(dash="dash", color="blue"),
    )
    return composite_return_fig


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
