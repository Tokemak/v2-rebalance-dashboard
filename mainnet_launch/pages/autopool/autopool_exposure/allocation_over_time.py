import pandas as pd
import streamlit as st
import plotly.express as px
from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS, WETH, USDC

from mainnet_launch.database.views import get_all_autopool_destinations, fetch_autopool_destination_state_df


def fetch_and_render_asset_allocation_over_time(autopool: AutopoolConstants):
    df = fetch_autopool_destination_state_df(autopool)

    end_of_day_safe_value_by_destination = (
        (
            df.groupby(["datetime", "readable_name"])["autopool_implied_safe_value"]
            .sum()
            .reset_index()
            .pivot(columns=["readable_name"], index=["datetime"], values="autopool_implied_safe_value")
        )
        .resample("1D")
        .last()
    )

    end_of_day_safe_value_by_asset = (
        (
            df.groupby(["datetime", "symbol"])["autopool_implied_safe_value"]
            .sum()
            .reset_index()
            .pivot(columns=["symbol"], index=["datetime"], values="autopool_implied_safe_value")
        )
        .resample("1D")
        .last()
    )

    percent_tvl_by_destination = 100 * end_of_day_safe_value_by_destination.div(
        end_of_day_safe_value_by_destination.sum(axis=1), axis=0
    )

    latest = percent_tvl_by_destination.tail(1).iloc[0]
    destinations_over_point_1_percent = latest[latest >= 0.1]

    st.plotly_chart(
        px.pie(
            values=destinations_over_point_1_percent.values,
            names=destinations_over_point_1_percent.index,
            title="Percent Allocation by Destination (≥.1%)",
        ),
        use_container_width=True,
    )

    st.plotly_chart(
        px.bar(
            end_of_day_safe_value_by_destination,
            title="TVL by Destination",
            labels={"value": autopool.base_asset_symbol},
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        px.bar(percent_tvl_by_destination, title="TVL Percent by Destination", labels={"value": "Percent"}),
        use_container_width=True,
    )

    st.plotly_chart(
        px.bar(end_of_day_safe_value_by_asset, title="TVL by Asset", labels={"value": autopool.base_asset_symbol}),
        use_container_width=True,
    )

    percent_tvl_by_asset = 100 * end_of_day_safe_value_by_asset.div(end_of_day_safe_value_by_asset.sum(axis=1), axis=0)

    st.plotly_chart(
        px.bar(percent_tvl_by_asset, title="TVL Percent by Asset", labels={"value": "Percent"}),
        use_container_width=True,
    )


if __name__ == "__main__":
    from mainnet_launch.constants import *

    fetch_and_render_asset_allocation_over_time(SILO_USD)
