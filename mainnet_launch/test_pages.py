import logging
import streamlit as st
import os
import subprocess
import time
import psutil

from mainnet_launch.ui_config_setup import config_plotly_and_streamlit
from mainnet_launch.constants import ALL_AUTOPOOLS
from mainnet_launch.page_functions import CONTENT_FUNCTIONS, PAGES_WITHOUT_AUTOPOOL

# run this with `$poetry run test-pages`

config_plotly_and_streamlit()

st.set_page_config(
    page_title="Mainnet Autopool Diagnostics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

testing_logger = logging.getLogger("testing_logger")
testing_logger.setLevel(logging.INFO)

# Only add the handler if it doesn't already exist
if not testing_logger.hasHandlers():
    handler = logging.FileHandler("verify_all_pages_work.log", mode="w")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    testing_logger.addHandler(handler)
    testing_logger.propagate = False


def get_memory_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    return mem_info.rss / (1024**2)


def open_log_in_vscode(log_file):
    if os.path.exists(log_file):
        try:
            subprocess.run(["code", log_file], check=True)
        except Exception as e:
            testing_logger.error(f"Could not open log file in VS Code: {e}")
    else:
        # Create the file if it doesn’t exist and then open it
        open(log_file, "w").close()
        subprocess.run(["code", log_file], check=True)


def log_and_time_function(page_name, func, autopool):
    start_time = time.time()
    if autopool is None:
        func()
    else:
        func(autopool)

    time_taken = time.time() - start_time
    if autopool is None:
        testing_logger.info(f"{time_taken:.2f} seconds | {page_name}")
    else:
        testing_logger.info(f"{time_taken:.2f} seconds | {page_name} |  {autopool.name}")
# uint256 swapCost = outEthValue.subSaturate(inEthValue);

def main():
    open_log_in_vscode("verify_all_pages_work.log")
    testing_logger.info("First run of page view and caching")

    start_time = time.time()
    for page_name, func in CONTENT_FUNCTIONS.items():
        if page_name in PAGES_WITHOUT_AUTOPOOL:
            log_and_time_function(page_name, func, autopool=None)
        else:
            for autopool in ALL_AUTOPOOLS:
                log_and_time_function(page_name, func, autopool=autopool)

    time_taken = time.time() - start_time
    usage = get_memory_usage()
    testing_logger.info(f"Fetched and Cached all pages {time_taken:.2f} seconds | Memory Usage: {usage:.2f} MB")

    testing_logger.info("Second run of page view and caching")

    start_time = time.time()
    for page_name, func in CONTENT_FUNCTIONS.items():
        if page_name in PAGES_WITHOUT_AUTOPOOL:
            log_and_time_function(page_name, func, autopool=None)
        else:
            for autopool in ALL_AUTOPOOLS:
                log_and_time_function(page_name, func, autopool=autopool)

    time_taken = time.time() - start_time
    usage = get_memory_usage()
    testing_logger.info(f"Fetched and Cached all pages {time_taken:.2f} seconds | Memory Usage: {usage:.2f} MB")
