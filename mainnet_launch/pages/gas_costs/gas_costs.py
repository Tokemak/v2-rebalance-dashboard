"""
Display gas costs for Tokemak's EOA addresses and Chainlink keeper network transactions

Only looks at mainnet
"""

import pandas as pd
import plotly.express as px
import streamlit as st


from mainnet_launch.constants import ETH_CHAIN, ChainData
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_transactions_table_for_gas_costs import (
    fetch_tokemak_address_constants_dfs,
)
from mainnet_launch.database.schema.postgres_operations import TableSelector, merge_tables_as_df
from mainnet_launch.database.schema.full import Blocks, ChainlinkGasCosts, Transactions


def _fetch_chainlink_keeper_network_transactions(chain: ChainData, chainlink_keepers_df: pd.DataFrame) -> pd.DataFrame:
    chainlink_gas_costs_df = merge_tables_as_df(
        [
            TableSelector(ChainlinkGasCosts, select_fields=[ChainlinkGasCosts.chainlink_topic_id]),
            TableSelector(
                Transactions,
                join_on=ChainlinkGasCosts.tx_hash == Transactions.tx_hash,
            ),
            TableSelector(Blocks, select_fields=Blocks.datetime, join_on=Transactions.block == Blocks.block),
        ],
        where_clause=Blocks.chain_id == chain.chain_id,
    )
    chainlink_gas_costs_df["gas_cost_in_eth"] = (
        chainlink_gas_costs_df["gas_cost_in_eth"] * 1.2
    )  # chainlink charges a 20% premium

    keeper_network_topic_ids_to_name = (
        chainlink_keepers_df[chainlink_keepers_df["chain_id"] == chain.chain_id].set_index("id")["name"].to_dict()
    )

    chainlink_gas_costs_df["category"] = "keeper_network"
    chainlink_gas_costs_df["label"] = chainlink_gas_costs_df["chainlink_topic_id"].map(keeper_network_topic_ids_to_name)
    return chainlink_gas_costs_df[
        ["datetime", "tx_hash", "label", "category", "effective_gas_price", "gas_used", "gas_cost_in_eth"]
    ]


def _fetch_eoa_transactions(
    chain: ChainData, deployers_df: pd.DataFrame, service_accounts_df: pd.DataFrame
) -> pd.DataFrame:
    address_to_name = service_accounts_df.set_index("address")["name"].to_dict()
    address_to_name[deployers_df["deployer"].values[0]] = "deployer"

    address_to_category = service_accounts_df.set_index("address")["type"].to_dict()
    address_to_category[deployers_df["deployer"].values[0]] = "deployer"

    eoa_tx_df = merge_tables_as_df(
        [
            TableSelector(
                Transactions,
                row_filter=Transactions.from_address.in_(list(address_to_name.keys())),
            ),
            TableSelector(Blocks, select_fields=Blocks.datetime, join_on=Transactions.block == Blocks.block),
        ],
        where_clause=Blocks.chain_id == chain.chain_id,
    )

    eoa_tx_df["label"] = eoa_tx_df["from_address"].map(address_to_name)
    eoa_tx_df["category"] = eoa_tx_df["from_address"].map(address_to_category)
    return eoa_tx_df[["datetime", "tx_hash", "label", "category", "effective_gas_price", "gas_used", "gas_cost_in_eth"]]


def fetch_our_gas_costs_df() -> pd.DataFrame:
    dfs = []
    deployers_df, chainlink_keepers_df, service_accounts_df = fetch_tokemak_address_constants_dfs()
    for chain in [ETH_CHAIN]:
        chainlink_gas_costs_df = _fetch_chainlink_keeper_network_transactions(chain, chainlink_keepers_df)
        eoa_tx_df = _fetch_eoa_transactions(chain, deployers_df, service_accounts_df)

        full_tx_df = pd.concat([chainlink_gas_costs_df, eoa_tx_df], ignore_index=True)
        dfs.append(full_tx_df)

    df = pd.concat([chainlink_gas_costs_df, eoa_tx_df], ignore_index=True).drop_duplicates()

    df["label"] = df["label"] + " (" + df["category"] + ")"

    address_to_name = service_accounts_df.set_index("address")["name"].to_dict()
    address_to_name[deployers_df["deployer"].values[0]] = "deployer"

    address_to_category = service_accounts_df.set_index("address")["type"].to_dict()
    address_to_category[deployers_df["deployer"].values[0]] = "deployer"

    # might be broken
    address_constants = (
        pd.DataFrame({"label": address_to_name, "category": address_to_category})
        # the dict‐keys become the row‐index automatically
        .rename_axis("address").reset_index()
    )
    label_to_all_time_gas_costs = df.groupby("label")["gas_cost_in_eth"].sum().to_dict()
    address_constants["all_time_total"] = address_constants["label"].map(label_to_all_time_gas_costs)

    return df, address_constants


def _render_gas_costs_charts(df: pd.DataFrame, address_constants: pd.DataFrame) -> None:
    for col in ["category", "label"]:
        st.subheader(f"Gas Costs in ETH and Gas Used by Expense {col}")
        for value_col in ["gas_cost_in_eth", "gas_used"]:
            daily_sum_df = (
                df.groupby([col, "datetime"])[value_col]
                .sum()
                .reset_index()
                .pivot(index="datetime", columns=col, values=value_col)
                .fillna(0)
                .resample("1D")
                .sum()
            )

            st.subheader(f"{value_col} by {col}")
            for n_days in [1, 7, 30]:
                fig = px.bar(
                    daily_sum_df.rolling(window=n_days).sum(),
                    title=f"{n_days}-day rolling sum {value_col}",
                )
                fig.update_yaxes(title_text=value_col)
                st.plotly_chart(fig, use_container_width=True)

    with st.expander("EOA addresses and all time costs"):
        st.dataframe(address_constants.sort_values("all_time_total", ascending=False))

    st.download_button(
        label="📥 Download gas costs as csv",
        data=df.to_csv(index=False),
        file_name="tokemak_eoa_gas_costs.csv",
        mime="text/csv",
    )


# todo

# add Etherscan,

# live gas used by day,

# avg daily gas price

# overlay this as a blue dashed line

# important, we are using a lot more gas than we were before
# the lower total gas prices comes from teh fact that hte network is less busy.
# and out timings


def fetch_and_render_gas_costs() -> None:
    df, address_constants = fetch_our_gas_costs_df()
    _render_gas_costs_charts(df, address_constants)

    # we should have an effective gas price chart
    # eg by day, how the weighted averge gas price we used

    # px.scatter(full_tx_df.set_index("datetime").resample("1d")["effective_gas_price"].agg(["median", "mean"]))

    # Liquidator_df["effective_gas_price_gwei"] = Liquidator_df["effective_gas_price"] / 1e9

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
