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
    compositeReturn_out_df = key_metric_data["compositeReturn_out_df"]

    st.header("Weighted Composite Return Metric")

    # Create line plots for each DataFrame and apply the default style
    composite_return_fig = px.line(compositeReturn_out_df, title=" ")
    _apply_default_style(composite_return_fig)
    composite_return_fig.update_traces(
        line=dict(width=8),
        selector=dict(name="balETH"),
        line_color="blue",
        line_dash="dash",
        line_width=3,
        marker=dict(size=10, symbol="circle", color="blue"),
    )
    composite_return_fig.update_traces(
        line=dict(width=8),
        selector=dict(name="autoETH"),
        line_color="blue",
        line_dash="dash",
        line_width=3,
        marker=dict(size=10, symbol="circle", color="blue"),
    )
    composite_return_fig.update_traces(
        line=dict(width=8),
        selector=dict(name="autoLRT"),
        line_color="blue",
        line_dash="dash",
        line_width=3,
        marker=dict(size=10, symbol="circle", color="blue"),
    )
    composite_return_fig.update_layout(yaxis_title="Composite Return (%)")

    # Layout for displaying the charts in Streamlit - stacked vertically
    st.plotly_chart(composite_return_fig, use_container_width=True)

    with st.expander("See explanation for Composite Metrics"):
        st.write(
            """
              Composite Return: Displays the composite returns calculated based on multiple factors.
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
