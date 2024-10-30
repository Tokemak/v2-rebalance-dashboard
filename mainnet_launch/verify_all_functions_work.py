import threading
import time
import logging
import datetime
import psutil
import streamlit as st
import os
import subprocess


from mainnet_launch.ui_config_setup import config_plotly_and_streamlit
from mainnet_launch.autopool_diagnostics.autopool_diagnostics_tab import (
    fetch_and_render_autopool_diagnostics_data,
    fetch_autopool_diagnostics_data,
)
from mainnet_launch.autopool_diagnostics.destination_allocation_over_time import (
    fetch_destination_allocation_over_time_data,
    fetch_and_render_destination_allocation_over_time_data,
)
from mainnet_launch.destination_diagnostics.weighted_crm import (
    fetch_weighted_crm_data,
    fetch_and_render_weighted_crm_data,
    fetch_and_render_destination_apr_data,
)
from mainnet_launch.solver_diagnostics.rebalance_events import (
    fetch_rebalance_events_data,
    fetch_and_render_rebalance_events_data,
)
from mainnet_launch.solver_diagnostics.solver_diagnostics import (
    fetch_and_render_solver_diagnositics_data,
    fetch_solver_diagnostics_data,
)
from mainnet_launch.top_level.key_metrics import fetch_key_metrics_data, fetch_and_render_key_metrics_data
from mainnet_launch.gas_costs.keeper_network_gas_costs import (
    fetch_keeper_network_gas_costs,
    fetch_and_render_keeper_network_gas_costs,
)
from mainnet_launch.constants import (
    CACHE_TIME,
    ALL_AUTOPOOLS,
    AUTOPOOL_NAME_TO_CONSTANTS,
    STREAMLIT_MARKDOWN_HTML,
    AutopoolConstants,
)

# Setup initial configurations
config_plotly_and_streamlit()
st.set_page_config(
    page_title="Mainnet Autopool Diagnostics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Set up primary and error logging
logging.basicConfig(
    filename="verify_all_pages_work.log", filemode="w", format="%(asctime)s - %(message)s", level=logging.INFO
)
logging.getLogger("verify_all_pages")

# Content functions mapping
CONTENT_FUNCTIONS = {
    "Key Metrics": fetch_and_render_key_metrics_data,
    "Autopool Exposure": fetch_and_render_destination_allocation_over_time_data,
    "Autopool CRM": fetch_and_render_weighted_crm_data,
    "Rebalance Events": fetch_and_render_rebalance_events_data,
    "Autopool Diagnostics": fetch_and_render_autopool_diagnostics_data,
    "Destination Diagnostics": lambda autopool: fetch_and_render_destination_apr_data(autopool),
    "Solver Diagnostics": fetch_and_render_solver_diagnositics_data,
    "Gas Costs": fetch_and_render_keeper_network_gas_costs,
}

PAGES_WITHOUT_AUTOPOOL = ["Gas Costs"]


# Define utility functions
def log_memory_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    return mem_info.rss / (1024**2)


# Main verification function
def verify_all_pages():
    logging.info("Starting page verification for all autopools and pages.")
    for page, func in CONTENT_FUNCTIONS.items():
        if page in PAGES_WITHOUT_AUTOPOOL:
            try:
                func()
                logging.info(f"Successfully verified page: {page} without autopool.")
            except Exception as e:
                logging.error(f"Error on page {page} without autopool: {e}")
        else:
            for autopool in ALL_AUTOPOOLS:
                try:
                    func(autopool)
                    logging.info(f"Successfully verified page: {page} for autopool {autopool.name}.")
                except Exception as e:
                    logging.error(f"Error on page {page} for autopool {autopool.name}: {e}")

    logging.info("Completed verification for all pages and autopools.")


# Function to open the log file in VS Code
def open_log_in_vscode(log_file):
    if os.path.exists(log_file):
        try:
            subprocess.run(["code", log_file], check=True)
        except Exception as e:
            logging.error(f"Could not open log file in VS Code: {e}")
    else:
        # Create the file if it doesn’t exist and then open it
        open(log_file, "w").close()
        subprocess.run(["code", log_file], check=True)


if __name__ == "__main__":
    open_log_in_vscode("verify_all_pages_work.log")
    verify_all_pages()
