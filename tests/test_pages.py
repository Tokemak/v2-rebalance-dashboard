import logging
import streamlit as st
import os
import subprocess
import time
import psutil
import shutil
import traceback
import inspect
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from mainnet_launch.app.ui_config_setup import config_plotly_and_streamlit
from mainnet_launch.constants import ALL_AUTOPOOLS, TEST_LOG_FILE_NAME, AutopoolConstants
from mainnet_launch.pages.page_functions import CONTENT_FUNCTIONS, PAGES_WITHOUT_AUTOPOOL


for name, logger in logging.root.manager.loggerDict.items():
    if "streamlit" in name:
        logging.getLogger(name).disabled = True

config_plotly_and_streamlit()

st.set_page_config(
    page_title="Mainnet Autopool Diagnostics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

testing_logger = logging.getLogger("testing_logger")
testing_logger.setLevel(logging.INFO)

if not testing_logger.hasHandlers():
    handler = logging.FileHandler(TEST_LOG_FILE_NAME, mode="w")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    testing_logger.addHandler(handler)
    testing_logger.propagate = False


def open_log_in_vscode(log_file: str):
    if not os.path.exists(log_file):
        try:
            open(log_file, "w").close()
        except Exception as e:
            testing_logger.error(f"failed to create log file '{log_file}': {e}")
            return

    if shutil.which("code"):
        cmd = ["code", "-r", log_file]
    else:
        cmd = ["open", "-a", "Visual Studio Code", log_file]

    try:
        subprocess.run(cmd, check=True)
    except Exception as e:
        testing_logger.error(f"could not open log file in vs code: {e}")


def log_and_time_function(page_name, func, autopool: AutopoolConstants):
    start = time.time()
    try:
        signature = inspect.signature(func)
        if autopool is None:
            if len(signature.parameters) == 0:
                func()
            else:
                func(autopool=None)
        else:
            if "autopool" in signature.parameters:
                func(autopool=autopool)
            else:
                func(autopool)
    except Exception as e:
        stack_trace = traceback.format_exc()
        if autopool is None:
            testing_logger.info(f"function: {func.__name__} failed | page: {page_name}")
        else:
            testing_logger.info(f"function: {func.__name__} failed | page: {page_name} | autopool: {autopool.name}")
        testing_logger.info(f"exception: {e}")
        testing_logger.info("stack trace:\n" + stack_trace)
    finally:
        elapsed = time.time() - start
        if autopool is None:
            testing_logger.info(f"execution time: {elapsed:.2f} seconds | page: {page_name}")
        else:
            testing_logger.info(
                f"execution time: {elapsed:.2f} seconds | page: {page_name} | autopool: {autopool.name}"
            )


def build_tasks():
    tasks = []
    for page_name, func in CONTENT_FUNCTIONS.items():
        if page_name in PAGES_WITHOUT_AUTOPOOL:
            tasks.append((page_name, func, None))
        else:
            for autopool in ALL_AUTOPOOLS:
                tasks.append((page_name, func, autopool))
    return tasks


def run_task_with_logging(page_name, func, autopool):
    # ensure Streamlit script run context to silence missing context warnings
    try:
        ctx = get_script_run_ctx()
        if ctx is not None:
            add_script_run_ctx(ctx)
    except Exception:
        pass
    log_and_time_function(page_name, func, autopool)


def run_task_no_logging(page_name, func, autopool):
    # ensure Streamlit script run context to silence missing context warnings
    try:
        ctx = get_script_run_ctx()
        if ctx is not None:
            add_script_run_ctx(ctx)
    except Exception:
        pass

    try:
        func(autopool)
    except Exception as e:
        open_log_in_vscode(TEST_LOG_FILE_NAME)
        log_and_time_function(page_name, func, autopool)
        raise e


def verify_all_pages_work():
    with ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(run_task_no_logging, name, func, autopool): name for name, func, autopool in build_tasks()
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="verifying pages"):
            page = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ {page} failed: {e}")


def verify_all_pages_work_with_times():
    open_log_in_vscode(TEST_LOG_FILE_NAME)

    for run_number in ["1st", "2nd"]:
        start = time.time()
        for page_name, func, autopool in build_tasks():
            run_task_with_logging(page_name, func, autopool)
        duration = time.time() - start
        this_process_memory_usage_in_mb = psutil.Process().memory_info().rss / (1024**2)
        print(
            f"verify_all_pages_work_with_times() {run_number=} took {duration:.2f} seconds | memory usage: {this_process_memory_usage_in_mb:.2f} MB"
        )
