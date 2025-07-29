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
from mainnet_launch.pages.page_functions import (
    AUTOPOOL_CONTENT_FUNCTIONS,
    PROTOCOL_CONTENT_FUNCTIONS,
    CHAIN_SPECIFIC_FUNCTIONS,
)


# the chain specific functions are not easy to test
CONTENT_FUNCTIONS = {**AUTOPOOL_CONTENT_FUNCTIONS, **PROTOCOL_CONTENT_FUNCTIONS}

print("CHAIN_SPECIFIC_FUNCTIONS are not tested")

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


def build_protocol_tasks():
    """Tasks for pages that take no autopool argument."""
    return [(name, func) for name, func in PROTOCOL_CONTENT_FUNCTIONS.items()]


def build_autopool_tasks():
    """Tasks for pages that take an autopool argument."""
    return [(name, func, autopool) for name, func in AUTOPOOL_CONTENT_FUNCTIONS.items() for autopool in ALL_AUTOPOOLS]


def _ensure_streamlit_ctx():
    try:
        ctx = get_script_run_ctx()
        if ctx:
            add_script_run_ctx(ctx)
    except Exception:
        pass


def run_with_log(page_name, func, autopool=None):
    start = time.time()
    try:
        sig = inspect.signature(func)
        if autopool is None:
            func()
        else:
            # pass autopool either as positional or keyword
            if "autopool" in sig.parameters:
                func(autopool=autopool)
            else:
                func(autopool)
    except Exception as e:
        context = f" | autopool: {autopool.name}" if autopool else ""
        testing_logger.info(f"function: {func.__name__} failed | page: {page_name}{context}")
        testing_logger.info(f"exception: {e}")
        testing_logger.info("stack trace:\n" + traceback.format_exc())
    finally:
        elapsed = time.time() - start
        context = f" | autopool: {autopool.name}" if autopool else ""
        testing_logger.info(f"execution time: {elapsed:.2f}s | page: {page_name}{context}")


def run_no_log(page_name, func, autopool=None):
    _ensure_streamlit_ctx()
    try:
        # run normally
        if autopool is None:
            func()
        else:
            func(autopool=autopool) if "autopool" in inspect.signature(func).parameters else func(autopool)
    except Exception:
        # on error, open log and re-run to capture timing
        open_log_in_vscode(TEST_LOG_FILE_NAME)
        run_with_log(page_name, func, autopool)
        raise


def verify_protocol_pages():
    """Verify all protocol-wide pages (no autopool)."""
    with ThreadPoolExecutor() as ex:
        futures = {ex.submit(run_no_log, name, func): name for name, func in build_protocol_tasks()}
        for future in tqdm(as_completed(futures), total=len(futures), desc="verifying protocol pages"):
            page = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Protocol page {page} failed: {e}")


def verify_autopool_pages():
    """Verify all autopool-specific pages (takes autopool)."""
    with ThreadPoolExecutor() as ex:
        futures = {
            ex.submit(run_no_log, name, func, autopool): f"{name} [{autopool.name}]" for name, func, autopool in ()
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="verifying autopool pages"):
            page = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"❌ Autopool page {page} failed: {e}")


def verify_all_pages_work():
    verify_protocol_pages()
    verify_autopool_pages()


def verify_all_pages_work_with_times():
    open_log_in_vscode(TEST_LOG_FILE_NAME)

    for run_number in ["1st", "2nd"]:
        start = time.time()
        for page_name, func, autopool in build_autopool_tasks():
            run_with_log(page_name, func, autopool)

        for page_name, func in build_protocol_tasks():
            run_with_log(page_name, func)

        duration = time.time() - start
        this_process_memory_usage_in_mb = psutil.Process().memory_info().rss / (1024**2)
        print(
            f"verify_all_pages_work_with_times() {run_number=} took {duration:.2f} seconds | memory usage: {this_process_memory_usage_in_mb:.2f} MB"
        )


if __name__ == "__main__":
    verify_all_pages_work_with_times()
