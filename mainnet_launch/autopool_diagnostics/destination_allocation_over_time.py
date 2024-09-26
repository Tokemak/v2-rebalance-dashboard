import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.destination_diagnostics.weighted_crm import fetch_weighted_crm_data
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use
from mainnet_launch.destination_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats


def display_destination_allocation_over_time(autopool: AutopoolConstants):
    blocks = build_blocks_to_use()
    uwcr_df, allocation_df, compositeReturn_out_df, total_nav_df, summary_stats_df, points_df = (
        fetch_destination_summary_stats(blocks, autopool)
    )

    percent_allocation_df = 100 * allocation_df.div(total_nav_df, axis=0)

    nav = round(allocation_df.tail(1).sum(), 2)
    st.header("Autopool Allocation By Destination")

    laster_percent_allocation = percent_allocation_df.tail(1)
    non_zero_allocation = laster_percent_allocation.loc[:, (laster_percent_allocation != 0).any(axis=0)]

    # Create pie chart using plotly with non-zero values
    pie_allocation_fig = px.pie(
        values=non_zero_allocation.iloc[0],
        names=non_zero_allocation.columns,
        title=f"{autopool.name} % allocation at {non_zero_allocation.index[0]} of {nav} total ETH",
    )
    
    allocation_fig = px.bar(allocation_df, title=f"{autopool.name}: Total ETH Value of TVL by Destination")
    allocation_fig.update_layout(yaxis_title="ETH")

    percent_allocation_fig = px.bar(percent_allocation_df, title=f"{autopool.name}: Percent of TVL by Destination")
    percent_allocation_fig.update_layout(yaxis_title="ETH")

    st.plotly_chart(pie_allocation_fig, use_container_width=True)
    st.plotly_chart(allocation_fig, use_container_width=True)
    st.plotly_chart(percent_allocation_fig, use_container_width=True)

    with st.expander("See explanation for Autopool Allocation Over Time"):
        st.write(
            """
            - Percent of ETH value by Destination at the current time
            - Total ETH Value of TVL by Destination: Shows the ETH value of capital deployed to each destination
            - Percent of TVL by Destination: Shows the percent of capital deployed to each destination
            """
        )


if __name__ == "__main__":
    display_destination_allocation_over_time(ALL_AUTOPOOLS[0])
    pass
