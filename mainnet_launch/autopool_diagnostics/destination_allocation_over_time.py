import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import CACHE_TIME, AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use
from mainnet_launch.destination_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats


st.cache_data(ttl=CACHE_TIME)


def fetch_destination_allocation_over_time_data(autopool: AutopoolConstants):
    blocks = build_blocks_to_use()
    uwcr_df, allocation_df, compositeReturn_out_df, total_nav_series, summary_stats_df, priceReturn_df = (
        fetch_destination_summary_stats(blocks, autopool)
    )

    percent_allocation_df = 100 * allocation_df.div(total_nav_series, axis=0)
    laster_percent_allocation = percent_allocation_df.tail(1)

    pie_allocation_fig = px.pie(
        values=laster_percent_allocation.iloc[0],
        names=laster_percent_allocation.columns,
        title=f"{autopool.name}% Allocation by Destination",
    )

    allocation_fig = px.bar(allocation_df, title=f"{autopool.name}: Total ETH Value of TVL by Destination")
    allocation_fig.update_layout(yaxis_title="ETH")

    percent_allocation_fig = px.bar(percent_allocation_df, title=f"{autopool.name}: Percent of TVL by Destination")
    percent_allocation_fig.update_layout(yaxis_title="ETH")

    return pie_allocation_fig, allocation_fig, percent_allocation_fig


def fetch_and_render_destination_allocation_over_time_data(autopool: AutopoolConstants):
    pie_allocation_fig, allocation_fig, percent_allocation_fig = fetch_destination_allocation_over_time_data(autopool)

    st.header(f"{autopool.name} Allocation By Destination")
    st.plotly_chart(pie_allocation_fig, use_container_width=True)
    st.plotly_chart(allocation_fig, use_container_width=True)
    st.plotly_chart(percent_allocation_fig, use_container_width=True)
    # low priority, add token exposure, from lens contract

    with st.expander("See explanation for Autopool Allocation Over Time"):
        st.write(
            """
            - Percent of ETH value by Destination at the current time
            - Total ETH Value of TVL by Destination: Shows the ETH value of capital deployed to each destination
            - Percent of TVL by Destination: Shows the percent of capital deployed to each destination
            """
        )
