import streamlit as st

# this needs to be first because otherwise we get this error:
# `StreamlitAPIException: set_page_config() can only be called once per app page,
# and must be called as the first Streamlit command in your script.`
st.set_page_config(
    page_title="Mainnet Autopool Diagnostics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


import threading
import time
import datetime
import logging

from mainnet_launch.autopool_diagnostics.fees import display_autopool_fees
from mainnet_launch.autopool_diagnostics.deposits_and_withdrawals import display_autopool_deposit_withdraw_stats
from mainnet_launch.autopool_diagnostics.count_of_destinations import display_autopool_destination_counts
from mainnet_launch.autopool_diagnostics.turnover import display_autopool_turnover


from mainnet_launch.destination_diagnostics.weighted_crm import display_weighted_crm
from mainnet_launch.autopool_diagnostics.destination_allocation_over_time import (
    display_destination_allocation_over_time,
)

from mainnet_launch.solver_diagnostics.rebalance_events import display_rebalance_events
from mainnet_launch.solver_diagnostics.solver_diagnostics import (
    fetch_and_render_solver_diagnositics_data,
    fetch_solver_diagnostics_data,
)

from mainnet_launch.top_level.key_metrics import fetch_key_metrics_data, fetch_and_render_key_metrics_data

from mainnet_launch.constants import (
    ALL_AUTOPOOLS,
    AUTOPOOL_NAME_TO_CONSTANTS,
    STREAMLIT_MARKDOWN_HTML,
    AutopoolConstants,
)


logging.basicConfig(filename="data_caching.log", filemode="w", format="%(asctime)s - %(message)s", level=logging.INFO)

data_caching_functions = [fetch_solver_diagnostics_data, fetch_key_metrics_data]


def cache_data_loop():
    try:
        logging.info(f"Started cache_data_loop()")
        while True:
            all_caching_started = time.time()
            for autopool in ALL_AUTOPOOLS:
                autopool_start_time = time.time()
                for func in data_caching_functions:
                    function_start_time = time.time()
                    func(autopool)
                    time_taken = time.time() - function_start_time
                    logging.info(f"{time_taken:.2f} seconds: Cached {func.__name__}({autopool.name}) ")

                autopool_time_taken = time.time() - autopool_start_time
                logging.info(f"{autopool_time_taken:.2f} seconds: Cached {autopool.name}")

            all_autopool_time_taken = time.time() - all_caching_started
            logging.info(f"{all_autopool_time_taken:.2f} seconds: All Autopools Cached")
            logging.info(f"Finished Caching")
            logging.info(f"Start Sleeping")
            time.sleep(6 * 3600)  # Sleep for 6 hours
            logging.info(f"Finished Sleeping")

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}")
        logging.error("Stack Trace:", exc_info=True)
        logging.info(f"Cache data loop has ended at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        raise


def display_autopool_diagnostics(autopool: AutopoolConstants):
    display_autopool_fees(autopool)
    display_autopool_deposit_withdraw_stats(autopool)
    display_autopool_destination_counts(autopool)
    display_autopool_turnover(autopool)


def display_autopool_exposure(autopool: AutopoolConstants):
    display_destination_allocation_over_time(autopool)

    st.text(
        """ 

        - Token Exposure pie chart
        
        """
    )


def display_destination_diagnostics(autopool: AutopoolConstants):
    st.text(
        """
    - unweighted price return (discounts/premiums) over time
    - fee apr
    - base apr
    - incentive apr (in / out)
    - points hook output → autoLRT
    - Growth of a dollar per destination, (don’t auto compound rewards tokens, just let the rewards pile up and price at eth value)
    - Time between Incentive APR snapshots
    - LP safe price
    - LP spot price

    """
    )


CONTENT_FUNCTIONS = {
    "Key Metrics": fetch_and_render_key_metrics_data,
    "Autopool Diagnostics": display_autopool_diagnostics,
    "Autopool Exposure": display_autopool_exposure,
    "Autopool APRs": display_weighted_crm,
    "Destination Diagnostics": display_destination_diagnostics,
    "Rebalance Events": display_rebalance_events,
    "Solver Diagnostics": fetch_and_render_solver_diagnositics_data,
}


def main():

    st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
    st.title("Autopool Diagnostics Dashboard")

    st.sidebar.title("Navigation")

    names = [autopool.name for autopool in ALL_AUTOPOOLS]
    pool_name = st.sidebar.selectbox("Select Pool", names)
    autopool = AUTOPOOL_NAME_TO_CONSTANTS[pool_name]
    page = st.sidebar.radio("Go to", CONTENT_FUNCTIONS.keys())

    CONTENT_FUNCTIONS[page](autopool)


if "cache_thread_started" not in st.session_state:
    st.session_state.cache_thread_started = False

# Start the caching thread only once
if not st.session_state.cache_thread_started:
    fetch_thread = threading.Thread(target=cache_data_loop, daemon=True)
    fetch_thread.start()
    st.session_state.cache_thread_started = True  # Set


if __name__ == "__main__":
    main()
