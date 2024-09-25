import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st


from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.destination_diagnostics.fetch_weighted_crm_data import fetch_weighted_crm_data


def display_destination_allocation_over_time(autopool: AutopoolConstants):
    key_metric_data = fetch_weighted_crm_data(autopool)
    allocation_df = key_metric_data["allocation_df"]
    total_nav_df = key_metric_data["total_nav_df"]

    percent_allocation_df = 100 * allocation_df.div(total_nav_df, axis=0)

    st.header("Autopool Allocation Over Time By Destination")

    allocation_fig = px.bar(allocation_df, title=f"{autopool.name}: Total ETH Value of TVL by Destination")
    allocation_fig.update_layout(yaxis_title="ETH")

    percent_allocation_fig = px.bar(percent_allocation_df, title=f"{autopool.name}: Percent of TVL by Destination")
    percent_allocation_fig.update_layout(yaxis_title="ETH")

    st.plotly_chart(allocation_fig, use_container_width=True)
    st.plotly_chart(percent_allocation_fig, use_container_width=True)

    with st.expander("See explanation for Autopool Allocation Over Time"):
        st.write(
            """
            - Total ETH Value of TVL by Destination: Shows the ETH value of capital deployed to each destination
            - Percent of TVL by Destination: Shows the percent of capital deployed to each destination
            """
        )


if __name__ == "__main__":
    display_destination_allocation_over_time(ALL_AUTOPOOLS[0])
