import pandas as pd
import streamlit as st
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objs as go

from datetime import datetime, timedelta, timezone


from mainnet_launch.destinations import get_destination_details
from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS, AUTO_LRT
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.add_info_to_dataframes import (
    add_timestamp_to_df_with_block_column,
    add_transaction_gas_info_to_df_with_tx_hash,
)
from mainnet_launch.abis import AUTOPOOL_VAULT_ABI


from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    get_earliest_block_from_table_with_autopool,
    get_all_rows_in_table_by_autopool,
)

from mainnet_launch.database.should_update_database import (
    should_update_table,
)

DESTINATION_DEBT_REPORTING_EVENTS_TABLE = "DESTINATION_DEBT_REPORTING_EVENTS_TABLE"


def add_new_debt_reporting_events_to_table():
    if should_update_table(DESTINATION_DEBT_REPORTING_EVENTS_TABLE):

        for autopool in ALL_AUTOPOOLS:
            highest_block_already_fetched = get_earliest_block_from_table_with_autopool(
                DESTINATION_DEBT_REPORTING_EVENTS_TABLE, autopool
            )
            debt_reporting_events_df = _fetch_debt_reporting_events_from_an_external_source(
                autopool, highest_block_already_fetched
            )
            if debt_reporting_events_df is not None:
                write_dataframe_to_table(debt_reporting_events_df, DESTINATION_DEBT_REPORTING_EVENTS_TABLE)


def _fetch_debt_reporting_events_from_an_external_source(
    autopool: AutopoolConstants, highest_block_already_fetched: int
):
    vault_contract = autopool.chain.client.eth.contract(autopool.autopool_addr, abi=AUTOPOOL_VAULT_ABI)
    debt_reporting_events_df = fetch_events(
        vault_contract.events.DestinationDebtReporting,
        chain=autopool.chain,
        start_block=highest_block_already_fetched,
    )

    debt_reporting_events_df = add_timestamp_to_df_with_block_column(
        debt_reporting_events_df, autopool.chain
    ).reset_index()
    debt_reporting_events_df["eth_claimed"] = debt_reporting_events_df["claimed"] / 1e18  # claimed is in ETH
    vault_to_name = {d.vaultAddress: d.vault_name for d in get_destination_details(autopool)}
    debt_reporting_events_df["destinationName"] = debt_reporting_events_df["destination"].apply(
        lambda x: vault_to_name[x]
    )
    debt_reporting_events_df["autopool"] = autopool.name
    cols = ["eth_claimed", "hash", "destinationName", "autopool", "timestamp", "block", "log_index"]
    debt_reporting_events_df = debt_reporting_events_df[cols].copy()
    debt_reporting_events_df = add_transaction_gas_info_to_df_with_tx_hash(debt_reporting_events_df, autopool.chain)


def fetch_autopool_destination_debt_reporting_events(autopool: AutopoolConstants) -> pd.DataFrame:
    add_new_debt_reporting_events_to_table()
    debt_reporting_events_df = get_all_rows_in_table_by_autopool(DESTINATION_DEBT_REPORTING_EVENTS_TABLE, autopool)
    return debt_reporting_events_df


def fetch_and_render_autopool_rewardliq_plot(autopool: AutopoolConstants):
    debt_reporting_events_df = fetch_autopool_destination_debt_reporting_events(autopool)
    destination_cumulative_sum = debt_reporting_events_df.pivot_table(
        values="eth_claimed", columns="destinationName", index="timestamp", fill_value=0
    ).cumsum()
    cumulative_eth_claimed_area_plot = px.area(
        destination_cumulative_sum, title="Cumulative ETH value of rewards claimed by destination"
    )
    cumulative_eth_claimed_area_plot.update_layout(yaxis_title="ETH", xaxis_title="Date")

    individual_reward_claim_events_fig = px.scatter(
        debt_reporting_events_df,
        x=debt_reporting_events_df.index,
        y="eth_claimed",
        color="destinationName",
        size="eth_claimed",
        size_max=40,
        title="Individual reward claiming and liquidation events",
    )

    individual_reward_claim_events_fig.update_layout(yaxis_title="ETH", xaxis_title="Date")
    st.plotly_chart(cumulative_eth_claimed_area_plot, use_container_width=True)
    st.plotly_chart(individual_reward_claim_events_fig, use_container_width=True)
