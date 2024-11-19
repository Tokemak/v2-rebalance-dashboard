import pytest
import time
from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants
from mainnet_launch.ui_config_setup import config_plotly_and_streamlit
import streamlit as st

from mainnet_launch.autopool_diagnostics.autopool_diagnostics_tab import (
    fetch_and_render_autopool_diagnostics_data,
)
from mainnet_launch.autopool_diagnostics.destination_allocation_over_time import (
    fetch_and_render_destination_allocation_over_time_data,
)
from mainnet_launch.destination_diagnostics.weighted_crm import (
    fetch_and_render_weighted_crm_data,
    fetch_and_render_destination_apr_data,
)
from mainnet_launch.solver_diagnostics.rebalance_events import (
    fetch_and_render_rebalance_events_data,
)
from mainnet_launch.solver_diagnostics.solver_diagnostics import (
    fetch_and_render_solver_diagnositics_data,
)
from mainnet_launch.top_level.key_metrics import fetch_and_render_key_metrics_data
from mainnet_launch.gas_costs.keeper_network_gas_costs import (
    fetch_and_render_keeper_network_gas_costs,
)
from mainnet_launch.accounting.incentive_token_liqudiation_prices import (
    fetch_and_render_reward_token_achieved_vs_incentive_token_price,
)
from mainnet_launch.accounting.protocol_level_profit import (
    fetch_and_render_protocol_level_profit_and_loss_data,
)

# Configure Plotly and Streamlit once before running tests
def setup_module(module):
    config_plotly_and_streamlit()
    st.set_page_config(
        page_title="Mainnet Autopool Diagnostics Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )

# Define the display function for Destination Diagnostics
def display_destination_diagnostics(autopool: AutopoolConstants):
    fetch_and_render_destination_apr_data(autopool)
    # Additional code can be added here if necessary

# Mapping of page names to their corresponding functions
CONTENT_FUNCTIONS = {
    "Key Metrics": fetch_and_render_key_metrics_data,
    "Autopool Exposure": fetch_and_render_destination_allocation_over_time_data,
    "Autopool CRM": fetch_and_render_weighted_crm_data,
    "Rebalance Events": fetch_and_render_rebalance_events_data,
    "Autopool Diagnostics": fetch_and_render_autopool_diagnostics_data,
    "Destination Diagnostics": display_destination_diagnostics,
    "Solver Diagnostics": fetch_and_render_solver_diagnositics_data,
    "Gas Costs": fetch_and_render_keeper_network_gas_costs,
    "Incentive Token Prices": fetch_and_render_reward_token_achieved_vs_incentive_token_price,
    "Protocol Level Profit and Loss": fetch_and_render_protocol_level_profit_and_loss_data,
}

# Pages that do not require an Autopool parameter
PAGES_WITHOUT_AUTOPOOL = [
    "Gas Costs",
    "Incentive Token Prices",
    "Protocol Level Profit and Loss",
]

# List of pages that require an Autopool parameter
pages_with_autopool = [
    p for p in CONTENT_FUNCTIONS.keys() if p not in PAGES_WITHOUT_AUTOPOOL
]

# Test functions for pages that require an Autopool parameter
@pytest.mark.parametrize("page_name", pages_with_autopool)
@pytest.mark.parametrize("autopool", ALL_AUTOPOOLS)
def test_pages_with_autopool(page_name, autopool):
    func = CONTENT_FUNCTIONS[page_name]
    start_time = time.time()
    func(autopool)
    time_taken = time.time() - start_time
    print(
        f"Execution Time: {time_taken:.2f} seconds | Page: {page_name} | Autopool: {autopool.name}"
    )

# Test functions for pages that do not require an Autopool parameter
@pytest.mark.parametrize("page_name", PAGES_WITHOUT_AUTOPOOL)
def test_pages_without_autopool(page_name):
    func = CONTENT_FUNCTIONS[page_name]
    start_time = time.time()
    func()
    time_taken = time.time() - start_time
    print(f"Execution Time: {time_taken:.2f} seconds | Page: {page_name}")
