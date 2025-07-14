import pandas as pd
import plotly.express as px
import streamlit as st

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_transactions_table_for_gas_costs import *


from mainnet_launch.database.schema.postgres_operations import TableSelector, merge_tables_as_df
from mainnet_launch.database.schema.full import Transactions, Blocks


def _fetch_gas_costs_df() -> pd.DataFrame:
    deployers_df, chainlink_keepers_df, service_accounts_df = fetch_systems_df()
    del chainlink_keepers_df

    service_accounts_df = service_accounts_df[service_accounts_df["chain_id"] == 1]
    deployers_df = deployers_df[deployers_df["chain_id"] == 1]

    address_to_name = service_accounts_df.set_index("address")["name"].to_dict()
    address_to_name[deployers_df["deployer"].values[0]] = "deployer"

    address_to_category = service_accounts_df.set_index("address")["type"].to_dict()
    address_to_category[deployers_df["deployer"].values[0]] = "deployer"

    full_tx_df = merge_tables_as_df(
        [
            TableSelector(
                Transactions,
                row_filter=Transactions.from_address.in_(
                    deployers_df["deployer"].tolist() + service_accounts_df["address"].tolist()
                ),
            ),
            TableSelector(Blocks, select_fields=Blocks.datetime, join_on=Transactions.block == Blocks.block),
        ],
        where_clause=Blocks.chain_id == 1,
    )

    full_tx_df["label"] = full_tx_df["from_address"].map(address_to_name)
    full_tx_df["category"] = full_tx_df["from_address"].map(address_to_category)
    full_tx_df["hour"] = full_tx_df["datetime"].dt.hour

    address_constants = (
        pd.DataFrame({"name": address_to_name, "category": address_to_category})
        # the dict‐keys become the row‐index automatically
        .rename_axis("address").reset_index()  # name the index  # turn index into a column
    )
    all_time_total = full_tx_df.groupby("from_address")["gas_cost_in_eth"].sum()
    address_constants["all_time_total"] = address_constants["address"].map(all_time_total)
    return full_tx_df, address_constants


def _render_gas_costs_charts(full_tx_df: pd.DataFrame, address_constants: pd.DataFrame) -> None:
    for col in ["category", "label"]:
        df = (
            full_tx_df.groupby([col, "datetime"])["gas_cost_in_eth"]
            .sum()
            .reset_index()
            .pivot(index="datetime", columns=col, values="gas_cost_in_eth")
            .fillna(0)
            .resample("1D")
            .sum()
        )

        seven_day_rolling_fig = px.bar(
            df.rolling(window=7).sum(),
            title="7-day rolling sum Mainnet Tokemak's EOA gas costs",
        )
        seven_day_rolling_fig.update_yaxes(title_text="ETH")

        thirty_day_rolling_fig = px.bar(
            df.rolling(window=30).sum(),
            title="30-day rolling sum Mainnet Tokemak's EOA gas costs",
        )
        thirty_day_rolling_fig.update_yaxes(title_text="ETH")

        st.subheader(f"Gas costs by EOA address {col}")
        st.plotly_chart(seven_day_rolling_fig, use_container_width=True)
        st.plotly_chart(thirty_day_rolling_fig, use_container_width=True)

    with st.expander("EOA addresses and all time costs"):
        st.dataframe(address_constants.sort_values("all_time_total", ascending=False))

    st.download_button(
        label="📥 Download EOA gas costs as CSV",
        data=full_tx_df.to_csv(index=False),
        file_name="tokemak_eoa_gas_costs.csv",
        mime="text/csv",
    )


def fetch_and_render_gas_costs() -> None:
    full_tx_df, address_constants = _fetch_gas_costs_df()
    _render_gas_costs_charts(full_tx_df, address_constants)
    # px.scatter(full_tx_df.set_index("datetime").resample("1d")["effective_gas_price"].agg(["median", "mean"]))

    #     Liquidator_df["effective_gas_price_gwei"] = Liquidator_df["effective_gas_price"] / 1e9

    # fig = px.box(
    #     Liquidator_df,
    #     x="hour",
    #     y="effective_gas_price_gwei",
    #     # points='all',            # show all underlying points
    #     title="Distribution of Effective Gas Price by Hour",
    #     labels={"hour": "Hour of Day", "effective_gas_price_gwei": "Effective Gas Price (Gwei)"},
    # )
    # fig.update_layout(
    #     yaxis_title="Effective Gas Price (Gwei)",
    #     xaxis_title="Hour of Day",
    #     boxmode="group",  # ensures boxes don’t overlap if you add more traces
    # )
    # todos?
    # add notes on methodology
    # add gas price to it as well
    # what if gas prices were always X?

    # we want to seperate out the impact of gas prices from this

    # we can save money

    # weekly distribtuion of gas prices
    # daily gas prices


if __name__ == "__main__":
    fetch_and_render_gas_costs()
