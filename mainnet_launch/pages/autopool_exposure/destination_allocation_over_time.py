import plotly.express as px
import streamlit as st


from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats


def fetch_destination_allocation_over_time_data(autopool: AutopoolConstants):
    pricePerShare_df = fetch_destination_summary_stats(autopool, "pricePerShare")
    ownedShares_df = fetch_destination_summary_stats(autopool, "ownedShares")
    allocation_df = pricePerShare_df * ownedShares_df
    percent_allocation_df = 100 * allocation_df.div(allocation_df.sum(axis=1), axis=0)

    latest_percent_allocation = percent_allocation_df.tail(1)

    pie_allocation_fig = px.pie(
        values=latest_percent_allocation.iloc[0],
        names=latest_percent_allocation.columns,
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
