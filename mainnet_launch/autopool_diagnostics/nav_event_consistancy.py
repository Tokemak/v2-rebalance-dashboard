import json
import os
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import streamlit as st

from mainnet_launch.constants import (
    CACHE_TIME,
    AutopoolConstants,
    ALL_AUTOPOOLS,
    eth_client,
    SOLVER_REBALANCE_PLANS_DIR,
    AUTO_ETH,
)
from mainnet_launch.abis.abis import AUTOPOOL_VAULT_ABI, AUTOPOOL_ETH_STRATEGY_ABI
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.solver_diagnostics.fetch_rebalance_events import (
    fetch_and_clean_rebalance_between_destination_events,
)
from mainnet_launch.destinations import attempt_destination_address_to_vault_name
from mainnet_launch.data_fetching.get_state_by_block import (
    add_timestamp_to_df_with_block_column,
)
import boto3
from botocore import UNSIGNED
from botocore.client import Config

from mainnet_launch.constants import CACHE_TIME, SOLVER_REBALANCE_PLANS_DIR, ALL_AUTOPOOLS


@st.cache_data(ttl=CACHE_TIME)
def _fetch_nav_event_figure(autopool: AutopoolConstants):
    vault_contract = eth_client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
    nav_df = fetch_events(vault_contract.events.Nav)
    nav_df = add_timestamp_to_df_with_block_column(nav_df)
    hours_since_last_nav_event_fig = _make_hours_since_last_nav_event_plot(nav_df)
    return hours_since_last_nav_event_fig


def _make_hours_since_last_nav_event_plot(nav_df: pd.DataFrame):
    time_diff_hours = nav_df.index.diff().dt.total_seconds() / 3600
    time_diff_hours.index = nav_df["date"]
    hours_since_last_nav_event_fig = px.scatter(
        time_diff_hours,
        labels={"value": "Hours", "index": "Date"},
        title="Hours Since Last Nav Event",
        height=600,
        width=600 * 3,
    )
    hours_since_last_nav_event_fig.add_hline(
        y=24, line_dash="dash", line_color="red", annotation_text="24-hour threshold", annotation_position="top right"
    )
    hours_since_last_nav_event_fig.update_yaxes(range=[20, 25])  # hours 320, 2
    return hours_since_last_nav_event_fig
