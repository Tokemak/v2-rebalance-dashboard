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

from mainnet_launch.constants import (
    CACHE_TIME,
    ALL_AUTOPOOLS,
    AUTOPOOL_NAME_TO_CONSTANTS,
    STREAMLIT_MARKDOWN_HTML,
    AutopoolConstants,
)


logging.basicConfig(filename="data_caching.log", filemode="w", format="%(asctime)s - %(message)s", level=logging.INFO)

data_caching_functions = [
    fetch_solver_diagnostics_data,
    fetch_key_metrics_data,
    fetch_autopool_diagnostics_data,
    fetch_destination_allocation_over_time_data,
    fetch_weighted_crm_data,
    fetch_rebalance_events_data,
]


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
                    logging.info(f"{time_taken:.2f} \t seconds: Cached {func.__name__}({autopool.name}) ")

                autopool_time_taken = time.time() - autopool_start_time
                logging.info(f"{autopool_time_taken:.2f} \t seconds: Cached {autopool.name}")

            all_autopool_time_taken = time.time() - all_caching_started
            logging.info(f"{all_autopool_time_taken:.2f} \t seconds: All Autopools Cached")
            logging.info(f"Finished Caching")
            logging.info(f"Start Sleeping")
            time.sleep(CACHE_TIME + (60 * 5))  # cache everything at least once every 6:05 hours
            logging.info(f"Finished Sleeping")

    except Exception as e:
        logging.error(f"Exception occurred: {str(e)}")
        logging.error("Stack Trace:", exc_info=True)
        logging.info(f"Cache data loop has ended at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        raise


def display_destination_diagnostics(autopool: AutopoolConstants):
    # a chart of

    # composite return out

    # composite retun in

    # price, fee, incentive points points

    # for all the destinations
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
    - LP spot price # lens contract?
    """
    )


CONTENT_FUNCTIONS = {
    "Key Metrics": fetch_and_render_key_metrics_data,
    "Autopool Diagnostics": fetch_and_render_autopool_diagnostics_data,
    "Autopool Exposure": fetch_and_render_destination_allocation_over_time_data,
    "Autopool APRs": fetch_and_render_weighted_crm_data,
    "Destination Diagnostics": display_destination_diagnostics,
    "Rebalance Events": fetch_and_render_rebalance_events_data,
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
