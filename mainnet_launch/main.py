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

import psutil


def get_memory_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    return mem_info.rss / (1024**2)


from mainnet_launch.constants import (
    CACHE_TIME,
    ALL_AUTOPOOLS,
    AUTOPOOL_NAME_TO_CONSTANTS,
    STREAMLIT_MARKDOWN_HTML,
    AutopoolConstants,
)


logging.basicConfig(filename="data_caching.log", filemode="w", format="%(asctime)s - %(message)s", level=logging.INFO)

per_autopool_data_caching_functions = [
    fetch_solver_diagnostics_data,
    fetch_key_metrics_data,
    fetch_autopool_diagnostics_data,
    fetch_destination_allocation_over_time_data,
    fetch_weighted_crm_data,
    fetch_rebalance_events_data,
]


not_per_autopool_data_caching_functions = [fetch_keeper_network_gas_costs]  # does not take any input variables


def log_and_time_function(func, *args):
    start_time = time.time()
    func(*args)
    time_taken = time.time() - start_time
    usage = get_memory_usage()
    if args:
        autopool = args[0]
        logging.info(
            f"{time_taken:.2f} seconds | Memory Usage: {usage:.2f} MB |Cached {func.__name__}({autopool.name})"
        )
    else:
        logging.info(f"{time_taken:.2f} seconds | Memory Usage: {usage:.2f} MB | Cached {func.__name__}()")


def cache_autopool_data():
    all_caching_started = time.time()
    logging.info("Start Autopool Functions")
    for autopool in ALL_AUTOPOOLS:
        autopool_start_time = time.time()
        for func in per_autopool_data_caching_functions:
            log_and_time_function(func, autopool)
        logging.info(f"{time.time() - autopool_start_time:.2f} seconds: Cached {autopool.name}")

    logging.info(f"{time.time() - all_caching_started:.2f} seconds: All Autopools Cached")


def cache_network_data():
    logging.info("Start Network Functions")
    network_start_time = time.time()
    for func in not_per_autopool_data_caching_functions:
        log_and_time_function(func)
    logging.info(f"{time.time() - network_start_time:.2f} seconds: Cached Network Functions")


def cache_data_loop():
    logging.info("Started cache_data_loop()")

    try:
        while True:
            all_caching_started = time.time()
            cache_network_data()
            cache_autopool_data()
            logging.info(f"{time.time() - all_caching_started:.2f} seconds: Everything Cached")
            logging.info("Finished Caching, Starting Sleep")
            time.sleep(CACHE_TIME + (60 * 5))
            logging.info("Finished Sleeping")
    except Exception:
        logging.exception("Exception occurred in cache_data_loop.")
        logging.info(f"Cache data loop ended at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        raise


def display_destination_diagnostics(autopool: AutopoolConstants):
    fetch_and_render_destination_apr_data(autopool)
    # a chart of

    # composite return out

    # composite retun in

    # price, fee, incentive points points

    # for all the destinations


CONTENT_FUNCTIONS = {
    "Key Metrics": fetch_and_render_key_metrics_data,
    "Autopool Exposure": fetch_and_render_destination_allocation_over_time_data,
    "Autopool CRM": fetch_and_render_weighted_crm_data,
    "Rebalance Events": fetch_and_render_rebalance_events_data,
    "Autopool Diagnostics": fetch_and_render_autopool_diagnostics_data,
    "Destination Diagnostics": display_destination_diagnostics,
    "Solver Diagnostics": fetch_and_render_solver_diagnositics_data,
    "Gas Costs": fetch_and_render_keeper_network_gas_costs,
}

PAGES_WITHOUT_AUTOPOOL = ["Gas Costs"]


def main():
    st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
    st.title("Autopool Diagnostics Dashboard")
    st.sidebar.title("Navigation")

    names = [autopool.name for autopool in ALL_AUTOPOOLS]
    pool_name = st.sidebar.selectbox("Select Pool", names)
    autopool = AUTOPOOL_NAME_TO_CONSTANTS[pool_name]

    page = st.sidebar.radio("Go to", CONTENT_FUNCTIONS.keys())

    if page in PAGES_WITHOUT_AUTOPOOL:
        CONTENT_FUNCTIONS[page]()
    else:
        CONTENT_FUNCTIONS[page](autopool)


thread_lock = threading.Lock()


def start_cache_thread():
    # Ensure this function only creates a thread if none exists
    if "cache_thread_started" not in st.session_state:
        st.session_state["cache_thread_started"] = False

    with thread_lock:
        if not st.session_state["cache_thread_started"]:
            fetch_thread = threading.Thread(target=cache_data_loop, daemon=True)
            fetch_thread.start()
            st.session_state["cache_thread_started"] = True
            st.session_state["fetch_thread"] = fetch_thread


def start_cache_thread():
    # this needs to be in a seperate function so that it won't run again on each refresh.
    # I don't know why this is
    # but it the below code block is not in a function then a new thread is created on each refresh
    if "cache_thread_started" not in st.session_state:
        st.session_state["cache_thread_started"] = False

    if not st.session_state["cache_thread_started"]:
        fetch_thread = threading.Thread(target=cache_data_loop, daemon=True)
        fetch_thread.start()
        st.session_state["cache_thread_started"] = True


start_cache_thread()

if __name__ == "__main__":
    main()
