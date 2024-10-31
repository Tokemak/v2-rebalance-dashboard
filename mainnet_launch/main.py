from mainnet_launch.ui_config_setup import config_plotly_and_streamlit

config_plotly_and_streamlit()

import streamlit as st
import atexit
import threading
import time
import logging
import os
import psutil
import signal

from mainnet_launch.constants import ALL_AUTOPOOLS, ROOT_DIR
from mainnet_launch.page_functions import (
    CONTENT_FUNCTIONS,
    PAGES_WITHOUT_AUTOPOOL,
    NOT_PER_AUTOPOOL_DATA_CACHING_FUNCTIONS,
    PER_AUTOPOOOL_DATA_CACHING_FUNCTIONS,
)

from mainnet_launch.constants import (
    CACHE_TIME,
    ALL_AUTOPOOLS,
    AUTOPOOL_NAME_TO_CONSTANTS,
    STREAMLIT_MARKDOWN_HTML,
)


def get_memory_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    return mem_info.rss / (1024**2)


production_logger = logging.getLogger("testing_logger")
production_logger.setLevel(logging.INFO)

# Only add the handler if it doesn't already exist
if not production_logger.hasHandlers():
    handler = logging.FileHandler("data_caching.log", mode="w")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    production_logger.addHandler(handler)
    production_logger.propagate = False


cache_file_lock_check = ROOT_DIR / "cache_thread_already_running.lock"


def cleanup():
    if os.path.exists(cache_file_lock_check):
        os.remove(cache_file_lock_check)
        production_logger.info("cache_file_lock_check removed on program exit.")


atexit.register(cleanup)


def log_and_time_function(page_name, func, autopool):
    start_time = time.time()
    if autopool is None:
        func()
    else:
        func(autopool)

    time_taken = time.time() - start_time
    if autopool is None:
        production_logger.info(f"{time_taken:.2f} seconds | {func.__name__} |  {page_name}")
    else:
        production_logger.info(f"{time_taken:.2f} seconds | {func.__name__} |  {page_name} | {autopool.name}")


def _cache_autopool_data():
    all_caching_started = time.time()
    production_logger.info("Start Autopool Functions")
    for func in PER_AUTOPOOOL_DATA_CACHING_FUNCTIONS:
        for autopool in ALL_AUTOPOOLS:
            autopool_start_time = time.time()
            log_and_time_function("caching", func, autopool)
            production_logger.info(f"{time.time() - autopool_start_time:.2f} seconds: Cached {autopool.name}")

    production_logger.info(f"{time.time() - all_caching_started:.2f} seconds: All Autopools Cached")


def _cache_network_data():
    production_logger.info("Start Network Functions")
    network_start_time = time.time()
    for func in NOT_PER_AUTOPOOL_DATA_CACHING_FUNCTIONS:
        log_and_time_function("caching thread", func, None)
    production_logger.info(f"{time.time() - network_start_time:.2f} seconds: Cached Network Functions")


def _cache_data():
    all_caching_started = time.time()
    _cache_autopool_data()
    _cache_network_data()
    production_logger.info(f"{time.time() - all_caching_started:.2f} seconds: Everything Cached")
    production_logger.info("Finished Caching, Starting Sleep")


def cache_data_loop():
    production_logger.info("Started cache_data_loop()")
    try:
        while True:
            _cache_data()
            time.sleep(CACHE_TIME + (60 * 5))  # + 5 minutes
            production_logger.info("Finished Sleeping")
    except Exception as e:
        production_logger.exception("Exception occurred in cache_data_loop." + str(e) + type(e))
        production_logger.info(f"Cache data loop ended at {time.strftime('%Y-%m-%d %H:%M:%S')}")

        if os.path.exists(cache_file_lock_check):
            os.remove(cache_file_lock_check)
            production_logger.info("cache_file_lock_check removed on program exit.")
        else:
            production_logger.info("cache_file_lock_check not remobed because it already does not exist")
        raise e


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


def start_cache_thread():
    # Ensure this function only creates a thread if none exists
    if os.path.exists(cache_file_lock_check):
        production_logger.info(f"Not starting another thread because {cache_file_lock_check} already exists")
    else:
        production_logger.info(f"Starting First thread because {cache_file_lock_check} does not exist")
        with open(cache_file_lock_check, "w") as fout:
            pass  # just open the file and don't write anything to it

        fetch_thread = threading.Thread(target=cache_data_loop, daemon=True)
        fetch_thread.start()  # this thread should keep going as long as the program is running


if __name__ == "__main__":
    start_cache_thread()
    main()
