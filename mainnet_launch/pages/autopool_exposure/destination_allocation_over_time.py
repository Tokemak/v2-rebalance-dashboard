import pandas as pd
import streamlit as st
import plotly.express as px

from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.pages.solver_diagnostics.solver_rebalance_plans_to_summary_stats import (
    DESTINATION_BLOCK_TABLE,
)
from mainnet_launch.database.database_operations import run_read_only_query


def fetch_and_render_destination_allocation_over_time_data(autopool: AutopoolConstants):
    destination_allocation_df = run_read_only_query(
        f"""
    SELECT pool_safe_price * autopool_owned_shares as autopool_safe_destination_value, vault_name, block_timestamp FROM {DESTINATION_BLOCK_TABLE} where autopool = ?
    """,
        (autopool.name,),
    )
    destination_allocation_df = destination_allocation_df.groupby(["block_timestamp", "vault_name"]).sum().reset_index()
    destination_allocation_df = destination_allocation_df.pivot(
        index="block_timestamp", columns="vault_name", values="autopool_safe_destination_value"
    )
    destination_allocation_df.index = pd.to_datetime(destination_allocation_df.index, unit="s", utc=True)
    destination_allocation_df = destination_allocation_df.sort_index()
    destination_allocation_df = destination_allocation_df.resample("1d").last()
    destination_percent_allocation_df = 100 * destination_allocation_df.div(
        destination_allocation_df.sum(axis=1), axis=0
    )

    highest_day = destination_percent_allocation_df.iloc[-1]

    st.plotly_chart(
        px.pie(values=highest_day.values, names=highest_day.index, title=f"{autopool.name}% Allocation by Destination"),
        use_container_width=True,
    )

    st.plotly_chart(
        px.bar(
            destination_allocation_df,
            title="TVL Safe Value by Destination",
            labels={"yaxis_title": autopool.base_asset_symbol},
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        px.bar(destination_percent_allocation_df, title="TVL Percent by Destination", labels={"value": "Percent"}),
        use_container_width=True,
    )


if __name__ == "__main__":
    from mainnet_launch.constants import ALL_AUTOPOOLS, AUTO_USD

    # auto USD broken
    for a in ALL_AUTOPOOLS:
        fetch_and_render_destination_allocation_over_time_data(a)


# def fetch_destination_allocation_over_time_data(autopool: AutopoolConstants):
#     pricePerShare_df = fetch_destination_summary_stats(autopool, "pricePerShare")
#     ownedShares_df = fetch_destination_summary_stats(autopool, "ownedShares")
#     allocation_df = pricePerShare_df * ownedShares_df
#     percent_allocation_df = 100 * allocation_df.div(allocation_df.sum(axis=1), axis=0)

#     latest_percent_allocation = percent_allocation_df.tail(1)

#     pie_allocation_fig = px.pie(
#         values=latest_percent_allocation.iloc[0],
#         names=latest_percent_allocation.columns,
#         title=f"{autopool.name}% Allocation by Destination",
#     )

#     allocation_fig = px.bar(allocation_df, title=f"{autopool.name}: Total ETH Value of TVL by Destination")
#     allocation_fig.update_layout(yaxis_title="ETH")

#     percent_allocation_fig = px.bar(percent_allocation_df, title=f"{autopool.name}: Percent of TVL by Destination")
#     percent_allocation_fig.update_layout(yaxis_title="NAV (%)")

#     return pie_allocation_fig, allocation_fig, percent_allocation_fig


# def fetch_and_render_destination_allocation_over_time_data(autopool: AutopoolConstants):
#     pie_allocation_fig, allocation_fig, percent_allocation_fig = fetch_destination_allocation_over_time_data(autopool)

#     st.header(f"{autopool.name} Allocation By Destination")
#     st.plotly_chart(pie_allocation_fig, use_container_width=True)
#     st.plotly_chart(allocation_fig, use_container_width=True)
#     st.plotly_chart(percent_allocation_fig, use_container_width=True)

#     fetch_and_render_asset_allocation_over_time(autopool)

#     with st.expander("See explanation for Autopool Allocation Over Time"):
#         st.write(
#             """
#             - Percent of ETH value by Destination at the current time
#             - Total ETH Value of TVL by Destination: Shows the ETH value of capital deployed to each destination
#             - Percent of TVL by Destination: Shows the percent of capital deployed to each destination
#             """
#         )


# if __name__ == "__main__":
#     from mainnet_launch.constants import AUTO_ETH

#     fetch_and_render_asset_allocation_over_time(AUTO_ETH)
