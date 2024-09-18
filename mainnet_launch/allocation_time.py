import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.fetch_weighted_crm_data import fetch_weighted_crm_data


def display_allocation_time(autopool: AutopoolConstants):
    # Fetch the required data frames
    key_metric_data = fetch_weighted_crm_data(autopool)
    allocation_df = key_metric_data["allocation_df"]

    st.header("Autopool Allocation Over Time")

    # Create line plots for each DataFrame and apply the default style
    
    allocation_fig = px.bar(allocation_df, title="Allocation Data")
    # _apply_default_style(allocation_fig)
    allocation_fig.update_layout(yaxis_title="Allocation Values")
    st.plotly_chart(allocation_fig, use_container_width=True)

    with st.expander("See explanation for Composite Metrics"):
        st.write(
            """
            - Allocation Data: Shows the allocation values over time for different destinations.
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
