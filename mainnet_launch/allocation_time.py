import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants, eth_client
from mainnet_launch.fetch_weighted_crm_data import fetch_weighted_crm_data
from mainnet_launch.destinations import get_current_destinations_to_symbol


def display_allocation_time(autopool: AutopoolConstants):
    # Fetch the required data frames
    key_metric_data = fetch_weighted_crm_data(autopool)
    allocation_df = key_metric_data["allocation_df"]
    destination_to_symbol = get_current_destinations_to_symbol(eth_client.eth.block_number)
    allocation_df.columns = [
        destination_to_symbol[c] if c in destination_to_symbol else c for c in allocation_df.columns
    ]

    st.header("Autopool Allocation Over Time")

    # Create line plots for each DataFrame and apply the default style

    allocation_fig = px.bar(allocation_df, title="Allocation Data")
    allocation_fig.update_layout(yaxis_title="Allocation Values")
    st.plotly_chart(allocation_fig, use_container_width=True)

    with st.expander("See explanation for Composite Metrics"):
        st.write(
            """
            - Allocation Data: Shows the allocation values over time for different destinations.
            """
        )
