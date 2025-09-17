"""See script for dan"""

import pandas as pd
import streamlit as st
import plotly.express as px

from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.data_fetching.tokemak_subgraph import run_query_with_paginate
from mainnet_launch.database.views import fetch_autopool_destination_state_df
from mainnet_launch.pages.autopool.key_metrics.key_metrics import fetch_key_metrics_data


def _fetch_apr_data_from_subgraph(autopool: AutopoolConstants) -> pd.DataFrame:
    query = """
    query GetAutopoolDayData($address: String!, $first: Int!, $skip: Int!) {
        autopoolDayDatas(
        where: { id_contains_nocase: $address }
        orderBy: timestamp
        orderDirection: asc
        first: $first
        skip: $skip
       ) {
        date
        autopoolDay30MAApy
        autopoolApy
        rewarderApy
        }
    }
    """

    variables = {"address": autopool.autopool_eth_addr.lower()}
    df = run_query_with_paginate(autopool.chain.tokemak_subgraph_url, query, variables, "autopoolDayDatas")

    df["date"] = pd.to_datetime(df["date"], utc=True)
    df.set_index("date", inplace=True)

    df["rewarderApy"] = pd.to_numeric(df["rewarderApy"], errors="coerce") / 1e18 * 100
    df["autopoolDay30MAApy"] = (
        pd.to_numeric(df["autopoolDay30MAApy"], errors="coerce") / (10**autopool.base_asset_decimals) * 100
    )
    df["autopoolApy"] = pd.to_numeric(df["autopoolApy"], errors="coerce") / (10**autopool.base_asset_decimals) * 100

    return df


def _fetch_percent_allocation_at_the_end_of_each_day(autopool) -> pd.DataFrame:
    # copied from mainnet_launch/pages/autopool_exposure/allocation_over_time.py
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

    percent_tvl_by_destination = 100 * end_of_day_safe_value_by_destination.div(
        end_of_day_safe_value_by_destination.sum(axis=1).replace(0, None), axis=0
    )
    return percent_tvl_by_destination


def _fetch_APY_and_allocation_data(autopool: AutopoolConstants):
    """Fetches APY and allocation data for the given autopool."""
    apr_df = _fetch_apr_data_from_subgraph(autopool)

    (
        nav_per_share_df,
        total_nav_series,
        expected_return_series,
        portion_allocation_by_destination_df,
        highest_block_and_datetime,
        price_return_series,
    ) = fetch_key_metrics_data(autopool)

    expected_return_series.name = "expected_return"
    # in theory, we only need key metrics for this, but keeping it as is for now

    percent_tvl_by_destination = _fetch_percent_allocation_at_the_end_of_each_day(autopool)

    df = pd.merge(percent_tvl_by_destination, apr_df, left_index=True, right_index=True, how="left")
    df = pd.merge(df, expected_return_series, left_index=True, right_index=True, how="left")

    # baseUSD, sonicUSD and autoDOLA do not have autopoolAPY fields, so instead we use the 30 day moving average of autopoolDay30MAApy
    # this is because (to validate) they don't back out price return

    df["baseApy"] = df["expected_return"].where(df["autopoolApy"].isna(), df["autopoolApy"])
    df["total_display_apy"] = df["baseApy"] + df["rewarderApy"]

    columns_to_keep = ["total_display_apy", "baseApy", "rewarderApy", *percent_tvl_by_destination.columns]
    return percent_tvl_by_destination, df[columns_to_keep]


def _render_plots(autopool: AutopoolConstants, percent_tvl_by_destination: pd.DataFrame, df: pd.DataFrame):
    st.plotly_chart(
        px.bar(
            percent_tvl_by_destination,
            x=percent_tvl_by_destination.index,
            y=percent_tvl_by_destination.columns,
            title=f"{autopool.name} % Allocation Over Time",
        ).update_layout(xaxis_title="Date", yaxis_title="% Allocation"),
        use_container_width=True,
    )

    st.plotly_chart(
        px.line(
            df[
                [
                    "total_display_apy",
                ]
            ],
            title=f"{autopool.name} APY",
        ),
        use_container_width=True,
    )


def _render_readme():

    with st.expander("Readme"):
        st.markdown(
            """
                **Purpose**  

                **How to use**
                1. Pick an autopool.
                2. Click **Fetch & Render** (~15s).
                3. Review charts + table.
                4. Download CSV.
                5. Make a cool gif for Twitter.

                **`total_display_apy`**
                - Method:
                - First 30 days **expected_return**. (eg sum(expected return of a destination * percent of TVL in that destination))
                - Day 31+: 30-day realized **autopoolApy** from the subgraph.
                - If `autopoolApy` missing (baseUSD, sonicUSD, autoDOLA), fall back to **autopoolDay30MAApy** (same metric shown in the UI).
                - Add **rewarderApy**

                **Downloaded CSV**
                - Contains `total_display_apy`, `baseApy`, `rewarderApy`, and % allocation by destination.
                - `total_display_apy` is the APY shown by the Tokemak UI.
                - `baseApy` is the expected return for the first 30 days, and the 30-day actual APY after that.
                - `rewarderApy` is the APY from TOKE incentives according to the subgraph.
            """
        )


def fetch_and_render_autopool_apy_and_allocation_over_time(autopool: AutopoolConstants) -> None:
    _render_readme()
    percent_tvl_by_destination, df = _fetch_APY_and_allocation_data(autopool)
    _render_plots(autopool, percent_tvl_by_destination, df)

    st.download_button(
        label=f"Download {autopool.name} APY and Allocation Data",
        data=df.to_csv(index=True).encode("utf-8"),
        file_name=f"{autopool.name}_apy_and_allocation_over_time.csv",
        mime="text/csv",
    )


if __name__ == "__main__":
    from mainnet_launch.constants import ALL_AUTOPOOLS

    for a in ALL_AUTOPOOLS:
        print(f"Fetching data for {a.name}")
        fetch_and_render_autopool_apy_and_allocation_over_time(a)
