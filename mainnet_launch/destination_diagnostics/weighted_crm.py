import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.destination_diagnostics.fetch_weighted_crm_data import fetch_weighted_crm_data
from mainnet_launch.destinations import attempt_destination_address_to_symbol


def display_weighted_crm(autopool: AutopoolConstants):
    # Fetch the required data frames
    key_metric_data = fetch_weighted_crm_data(autopool)

    st.header("Destinations APR")

    composite_return_fig = _make_all_destination_composite_return_df(autopool, key_metric_data)
    st.plotly_chart(composite_return_fig, use_container_width=True)

    apr_components_fig = _make_apr_components_fig(key_metric_data)
    st.plotly_chart(apr_components_fig, use_container_width=True)

    with st.expander("See explanation"):
        st.write(
            f"""
              Composite Return Out: Composite Return out of the various destinations
              {autopool.name} Weighted Expected Return: sum(% of assets in Destination * Destination Composite Return). Idle ETH has 0% return
              APR Components: Show Fee, Incentive, Base and Price Return of the destination. Does not include points 
            """
        )


def _make_all_destination_composite_return_df(autopool: AutopoolConstants, key_metric_data: dict) -> go.Figure:
    allocation_df = key_metric_data["allocation_df"]
    total_nav_df = key_metric_data["total_nav_df"]
    compositeReturn_out_df = key_metric_data["compositeReturn_out_df"]
    portion_allocation_df = allocation_df.div(total_nav_df, axis=0)
    autopool_weighted_expected_return = (compositeReturn_out_df * portion_allocation_df).sum(axis=1)
    compositeReturn_out_df[f"{autopool.name} Weighted Expected Return"] = autopool_weighted_expected_return
    composite_return_fig = px.line(compositeReturn_out_df, title=f"{autopool.name} Destinations and composite Return")
    _apply_default_style(composite_return_fig)
    composite_return_fig.update_layout(yaxis_title="Composite Return (%)")
    composite_return_fig.update_traces(
        selector=dict(name=f"{autopool.name} Weighted Expected Return"),
        line=dict(dash="dash", color="blue"),
    )
    return composite_return_fig


def _make_apr_components_fig(key_metric_data: dict) -> go.Figure:
    summary_stats_df = key_metric_data["summary_stats_df"]
    summary_stats_df.columns = [attempt_destination_address_to_symbol(c) for c in summary_stats_df.columns]
    price_return_df = 100 * summary_stats_df.map(
        lambda row: row["priceReturn"] if isinstance(row, dict) else None
    ).astype(float)

    baseApr_df = 100 * summary_stats_df.map(lambda row: row["baseApr"] if isinstance(row, dict) else None).astype(float)

    incentiveApr_df = 100 * summary_stats_df.map(
        lambda row: row["incentiveApr"] if isinstance(row, dict) else None
    ).astype(float)

    feeApr_df = 100 * summary_stats_df.map(lambda row: row["feeApr"] if isinstance(row, dict) else None).astype(float)

    st.title("Destination APR Components")

    destination = st.selectbox("Select a destination", summary_stats_df.columns)

    points_df = 100 * key_metric_data["points_df"]

    # Extract selected data from each table
    plot_data = pd.DataFrame(
        {
            "Price Return": price_return_df[destination],
            "Base APR": baseApr_df[destination],
            "Incentive APR": incentiveApr_df[destination],
            "Fee APR": feeApr_df[destination],
            "Points APR": points_df[destination],
        },
        index=summary_stats_df.index,
    )

    apr_components_fig = px.line(plot_data, title=f"APR Components for {destination}")
    _apply_default_style(apr_components_fig)
    return apr_components_fig


def _apply_default_style(fig: go.Figure) -> None:
    """
    Applies a consistent default style to all Plotly figures.
    """
    fig.update_traces(line=dict(width=3))  # Line width remains consistent
    fig.update_layout(
        title_x=0.5,  # Center the title
        margin=dict(l=40, r=40, t=40, b=80),  # Adjust margins for better spacing
        height=500,  # Slightly increase the height
        width=1200,  # Make the graph wider
        font=dict(size=16),
        xaxis_title="",  # No title for X-axis
        plot_bgcolor="white",  # Transparent plot background
        paper_bgcolor="white",  # Transparent paper background
        xaxis=dict(showgrid=True, gridcolor="lightgray"),  # X-axis grid style
        yaxis=dict(showgrid=True, gridcolor="lightgray"),  # Y-axis grid style
        colorway=px.colors.qualitative.Set2,  # Apply a colorful theme
    )


if __name__ == "__main__":
    from mainnet_launch.constants import ALL_AUTOPOOLS, AUTO_LRT

    display_weighted_crm(AUTO_LRT)
