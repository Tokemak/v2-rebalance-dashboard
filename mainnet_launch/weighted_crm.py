import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.fetch_weighted_crm_data import fetch_weighted_crm_data


def display_weighted_crm(autopool: AutopoolConstants):
    # Fetch the required data frames
    key_metric_data = fetch_weighted_crm_data(autopool)
    uwcr_df = key_metric_data["uwcr_df"]
    allocation_df = key_metric_data["allocation_df"]
    total_nav_df = key_metric_data["total_nav_df"]
    compositeReturn_out_df = key_metric_data["compositeReturn_out_df"]
    portion_allocation_df = allocation_df.div(total_nav_df, axis=0)
    autopool_weighted_expected_return = (compositeReturn_out_df * portion_allocation_df).sum(axis=1)
    st.header("Weighted Composite Return Metric")
    compositeReturn_out_df[f"{autopool.name} Weighted Expected Return"] = autopool_weighted_expected_return
    composite_return_fig = px.line(compositeReturn_out_df, title=f"{autopool.name} Destinations and composite Return")
    _apply_default_style(composite_return_fig)
    composite_return_fig.update_layout(yaxis_title="Composite Return (%)")
    
    composite_return_fig.update_traces(
        selector=dict(name=f"{autopool.name} Weighted Expected Return"),  # Select the last trace by name
        line=dict(dash="dash", color="blue")  # Set line style to dashed and color to blue
    )
    
    st.plotly_chart(composite_return_fig, use_container_width=True)
    with st.expander("See explanation for Composite Metrics"):
        st.write(
            f"""
              Composite Return Out: CRM out of the various destinations
              {autopool.name} Weighted Expected Return: sum(% of assets in Destination * Destination Composite Return).  Idle ETH has 0% return
            """
        )


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
    from mainnet_launch.constants import ALL_AUTOPOOLS

    display_weighted_crm(ALL_AUTOPOOLS[0])
