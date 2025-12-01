"""
Display gas costs for Tokemak's EOA addresses and Chainlink keeper network transactions

Only looks at mainnet
"""

import pandas as pd
import plotly.express as px
import streamlit as st


from mainnet_launch.constants import ETH_CHAIN, ChainData, SessionState
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_gas_costs.update_transactions_table_for_gas_costs import (
    fetch_tokemak_address_constants_dfs,
)
from mainnet_launch.database.postgres_operations import TableSelector, merge_tables_as_df
from mainnet_launch.database.schema.full import Blocks, ChainlinkGasCosts, Transactions


def _fetch_chainlink_keeper_network_transactions(chain: ChainData, chainlink_keepers_df: pd.DataFrame) -> pd.DataFrame:
    if st.session_state.get(SessionState.RECENT_START_DATE):
        where_clause = (Blocks.chain_id == chain.chain_id) & (
            Blocks.datetime >= st.session_state[SessionState.RECENT_START_DATE]
        )
    else:
        where_clause = Blocks.chain_id == chain.chain_id

    chainlink_gas_costs_df = merge_tables_as_df(
        [
            TableSelector(ChainlinkGasCosts, select_fields=[ChainlinkGasCosts.chainlink_topic_id]),
            TableSelector(
                Transactions,
                join_on=ChainlinkGasCosts.tx_hash == Transactions.tx_hash,
            ),
            TableSelector(Blocks, select_fields=Blocks.datetime, join_on=Transactions.block == Blocks.block),
        ],
        where_clause=where_clause,
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
    if len(deployers_df) != 1:
        print(deployers_df)
        raise ValueError("Expected exactly one deployer address, found: {}".format(len(deployers_df)))

    if st.session_state.get(SessionState.RECENT_START_DATE):
        where_clause = (Blocks.chain_id == chain.chain_id) & (
            Blocks.datetime >= st.session_state[SessionState.RECENT_START_DATE]
        )
    else:
        where_clause = Blocks.chain_id == chain.chain_id

    address_to_name = service_accounts_df.set_index("address")["name"].to_dict()
    address_to_name[deployers_df["deployer"].values[0]] = "deployer" + " " + str(chain.chain_id)

    address_to_category = service_accounts_df.set_index("address")["type"].to_dict()
    address_to_category[deployers_df["deployer"].values[0]] = "deployer" + " " + str(chain.chain_id)

    our_eoa_addresses = list(address_to_name.keys())

    eoa_tx_df = merge_tables_as_df(
        [
            TableSelector(
                Transactions,
                row_filter=Transactions.from_address.in_(our_eoa_addresses),
            ),
            TableSelector(Blocks, select_fields=Blocks.datetime, join_on=Transactions.block == Blocks.block),
        ],
        where_clause=where_clause,
    )

    eoa_tx_df["label"] = eoa_tx_df["from_address"].map(address_to_name)
    eoa_tx_df["category"] = eoa_tx_df["from_address"].map(address_to_category)
    return eoa_tx_df[["datetime", "tx_hash", "label", "category", "effective_gas_price", "gas_used", "gas_cost_in_eth"]]


def fetch_our_gas_costs_df() -> pd.DataFrame:
    dfs = []
    deployers_df, chainlink_keepers_df, service_accounts_df = fetch_tokemak_address_constants_dfs()
    for chain in [ETH_CHAIN]:
        chainlink_gas_costs_df = _fetch_chainlink_keeper_network_transactions(
            chain, chainlink_keepers_df[chainlink_keepers_df["chain_id"] == chain.chain_id]
        )
        eoa_tx_df = _fetch_eoa_transactions(
            chain,
            deployers_df[deployers_df["chain_id"] == chain.chain_id],
            service_accounts_df[service_accounts_df["chain_id"] == chain.chain_id],
        )

        full_tx_df = pd.concat([chainlink_gas_costs_df, eoa_tx_df], ignore_index=True)
        dfs.append(full_tx_df)

    df = pd.concat([chainlink_gas_costs_df, eoa_tx_df], ignore_index=True).drop_duplicates()

    df["label"] = df["label"] + " (" + df["category"] + ")"

    all_time_totals = df.groupby("label")["gas_cost_in_eth"].sum()
    return df, all_time_totals, deployers_df, chainlink_keepers_df, service_accounts_df


def _pick_and_render_gas_used_and_gas_costs_charts(df: pd.DataFrame):
    col1, col2 = st.columns([1, 1])
    with col1:
        breakdown = st.selectbox("Group by Expense", ["category", "label"])
    with col2:
        metric = st.selectbox("Metric", ["gas_cost_in_eth", "gas_used"])

    st.header(f"{metric.replace('_',' ').title()} grouped by {breakdown}")

    daily = (
        df.groupby([breakdown, "datetime"])[metric]
        .sum()
        .reset_index()
        .pivot(index="datetime", columns=breakdown, values=metric)
        .fillna(0)
        .resample("1D")
        .sum()
    )

    for window in (1, 7, 30):
        st.subheader(f"{window}-day rolling sum")
        fig = px.bar(
            daily.rolling(window=window).sum(),
            labels={"value": metric, "datetime": "Date"},
            title=f"{window}-day rolling sum of {metric}",
        )
        st.plotly_chart(fig, use_container_width=True)


def _render_daily_gas_price_df(df: pd.DataFrame) -> None:
    daily_gas_prices_df = df.set_index("datetime").resample("1D")["effective_gas_price"].mean().reset_index()
    daily_gas_prices_df["effective_gas_price"] = daily_gas_prices_df["effective_gas_price"] / 1e9  # convert to Gwei

    fig = px.line(
        daily_gas_prices_df,
        x="datetime",
        y="effective_gas_price",
        title="(all our transactions) Average Daily Effective Gas Price (Gwei)",
        labels={"week": "Week", "effective_gas_price": "Effective Gas Price (Gwei)"},
    )

    st.plotly_chart(fig, use_container_width=True)


def fetch_and_render_gas_costs() -> None:
    st.subheader("Tokemak Mainnet Gas Costs")

    df, all_time_totals, deployers_df, chainlink_keepers_df, service_accounts_df = fetch_our_gas_costs_df()
    _pick_and_render_gas_used_and_gas_costs_charts(df)
    _render_daily_gas_price_df(df)

    with st.expander("See All Time Gas Costs in ETH"):
        st.dataframe(all_time_totals.sort_values(ascending=False))

    with st.expander("See Addresses"):
        st.markdown("**Deployer Addresses**")
        st.dataframe(deployers_df)

        st.markdown("**Chainlink Keepers**")
        st.dataframe(chainlink_keepers_df)

        st.markdown("**Tokemak's Service Accounts**")
        st.dataframe(service_accounts_df)

    st.download_button(
        label="📥 Download transactions gas costs as csv",
        data=df.to_csv(index=False),
        file_name="tokemak_gas_costs.csv",
        mime="text/csv",
    )

    with st.expander("Details"):
        st.markdown("so far only mainnet, can add other chains later")


if __name__ == "__main__":
    fetch_and_render_gas_costs()


# TODO

# add Etherscan,

# live gas used by day,

# avg daily gas price

# overlay this as a blue dashed line

# important, we are using a lot more gas than we were before
# the lower total gas prices comes from teh fact that hte network is less busy.
# and out timings


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
