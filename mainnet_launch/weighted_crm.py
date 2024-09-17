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

    st.header("Autopool Composite Data Metrics")

    # Create line plots for each DataFrame and apply the default style
    uwcr_fig = px.line(uwcr_df, title="Underlying Weighted Capital Return (UWCR)")
    _apply_default_style(uwcr_fig)
    uwcr_fig.update_layout(yaxis_title="UWCR (%)")

    allocation_fig = px.line(allocation_df, title="Allocation Data")
    _apply_default_style(allocation_fig)
    allocation_fig.update_layout(yaxis_title="Allocation Values")

    composite_return_fig = px.line(compositeReturn_out_df, title="Composite Return Data")
    _apply_default_style(composite_return_fig)
    composite_return_fig.update_layout(yaxis_title="Composite Return (%)")

    # Layout for displaying the charts in Streamlit - stacked vertically
    st.subheader("UWCR (%)")
    st.plotly_chart(uwcr_fig, use_container_width=True)

    st.subheader("Allocation Data")
    st.plotly_chart(allocation_fig, use_container_width=True)

    st.subheader("Composite Return (%)")
    st.plotly_chart(composite_return_fig, use_container_width=True)

    with st.expander("See explanation for Composite Metrics"):
        st.write(
            """
            This section provides insights into different metrics:
            - UWCR: Represents the Underlying Weighted Capital Return, which indicates returns adjusted by weighting.
            - Allocation Data: Shows the allocation values over time for different destinations.
            - Composite Return: Displays the composite returns calculated based on multiple factors.
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
        plot_bgcolor="rgba(0, 0, 0, 0)",  # Transparent plot background
        paper_bgcolor="rgba(0, 0, 0, 0)",  # Transparent paper background
        xaxis=dict(showgrid=True, gridcolor="lightgray"),  # X-axis grid style
        yaxis=dict(showgrid=True, gridcolor="lightgray"),  # Y-axis grid style
        colorway=px.colors.qualitative.Set2,  # Apply a colorful theme
    )
