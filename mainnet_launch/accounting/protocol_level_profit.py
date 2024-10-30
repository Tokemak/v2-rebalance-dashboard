import streamlit as st
import pandas as pd

from mainnet_launch.constants import CACHE_TIME
from mainnet_launch.gas_costs.keeper_network_gas_costs import (
    fetch_solver_gas_costs,
    fetch_keeper_network_gas_costs,
    fetch_all_autopool_debt_reporting_events,
)


from mainnet_launch.autopool_diagnostics.fees import fetch_autopool_fee_data
import pandas as pd
import plotly.express as px
from mainnet_launch.constants import (
    CACHE_TIME,
    eth_client,
    ALL_AUTOPOOLS,
    BAL_ETH,
    AUTO_ETH,
    AUTO_LRT,
    AutopoolConstants,
    WORKING_DATA_DIR,
)
from datetime import datetime, timedelta, timezone


st.cache_data(ttl=CACHE_TIME)


def fetch_gas_cost_df() -> pd.DataFrame:
    destination_debt_reporting_df = fetch_all_autopool_debt_reporting_events()
    rebalance_gas_cost_df = fetch_solver_gas_costs()
    keeper_gas_costs_df = fetch_keeper_network_gas_costs()

    gas_cost_columns = ["hash", "gas_cost_in_eth"]

    debt_reporting_costs = destination_debt_reporting_df[gas_cost_columns].copy().drop_duplicates()
    debt_reporting_costs.columns = ["hash", "debt_reporting_gas"]

    solver_costs = rebalance_gas_cost_df[gas_cost_columns].copy().drop_duplicates()
    solver_costs.columns = ["hash", "solver_gas"]

    keeper_costs = keeper_gas_costs_df[gas_cost_columns].copy().drop_duplicates()
    keeper_costs.columns = ["hash", "keeper_gas"]

    # sometimes the solver rebalancing causes destination debt reporting
    # to not double count those gas costs all the transactions where
    # TODO

    gas_cost_df = pd.concat([debt_reporting_costs, solver_costs, keeper_costs])
    return gas_cost_df.fillna(0)


st.cache_data(ttl=CACHE_TIME)


def fetch_fee_df() -> pd.DataFrame:
    """
    Fetch all the the fees in ETH from the feeCollected and PeriodicFeeCollected events for each autopool
    """
    fee_dfs = []
    for autopool in ALL_AUTOPOOLS:
        periodic_fee_df, streaming_fee_df = fetch_autopool_fee_data(autopool)
        periodic_fee_df.columns = [f"{autopool.name}_periodic"]
        streaming_fee_df.columns = [f"{autopool.name}_streaming"]
        fee_dfs.extend([periodic_fee_df, streaming_fee_df])

    fee_df = pd.concat(fee_dfs).fillna(0)
    return fee_df
