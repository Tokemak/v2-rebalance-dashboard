from mainnet_launch.app.ui_config_setup import config_plotly_and_streamlit

config_plotly_and_streamlit()

from datetime import datetime
import streamlit as st
import logging
import psutil
import os


from mainnet_launch.constants import ALL_AUTOPOOLS, PRODUCTION_LOG_FILE_NAME, DB_FILE
from mainnet_launch.pages.page_functions import (
    CONTENT_FUNCTIONS,
    PAGES_WITHOUT_AUTOPOOL,
)
from mainnet_launch.app.run_on_startup import first_run_of_db


STREAMLIT_MARKDOWN_HTML = """
        <style>
        .main {
            max-width: 85%;
            margin: 0 auto;
            padding-top: 40px;
        }
        .stPlotlyChart {
            width: 100%;
            height: auto;
            min-height: 300px;
            max-height: 600px;
            background-color: #f0f2f6;
            border-radius: 5px;
            padding: 20px;
        }
        @media (max-width: 768px) {
            .stPlotlyChart {
                min-height: 250px;
                max-height: 450px;
            }
        }
        .stPlotlyChart {
            background-color: #f0f2f6;
            border-radius: 5px;
            padding: 10px;
        }
        .stExpander {
            background-color: #e6e9ef;
            border-radius: 5px;
            padding: 10px;
        }
        </style>
        """


def get_memory_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    return mem_info.rss / (1024**2)


production_logger = logging.getLogger("production_logger")
production_logger.setLevel(logging.INFO)

# Only add the handler if it doesn't already exist
if not production_logger.hasHandlers():
    handler = logging.FileHandler(PRODUCTION_LOG_FILE_NAME, mode="w")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    production_logger.addHandler(handler)
    production_logger.propagate = False


def format_timedelta(td):
    """Format a timedelta object into a readable string."""
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    milliseconds = int(td.microseconds / 1000)
    formatted_time = ""
    if hours > 0:
        formatted_time += f"{hours}h "
    if minutes > 0 or hours > 0:
        formatted_time += f"{minutes}m "
    formatted_time += f"{seconds}s {milliseconds}ms"
    return formatted_time


def main():

    if not os.path.exists(DB_FILE):
        first_run_of_db()
        with open("db_initalized", "x") as _:
            pass

    if os.path.exists("db_initalized"):
        st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
        st.title("Autopool Diagnostics Dashboard")
        st.sidebar.title("Navigation")

        names = [autopool.name for autopool in ALL_AUTOPOOLS]
        pool_name = st.sidebar.selectbox("Select Pool", names)
        autopool_name_to_constants = {a.name: a for a in ALL_AUTOPOOLS}
        autopool = autopool_name_to_constants[pool_name]

        page = st.sidebar.radio("Go to", CONTENT_FUNCTIONS.keys())

        if page in PAGES_WITHOUT_AUTOPOOL:
            start = datetime.now()
            production_logger.info(f"Attempting {page}")
            CONTENT_FUNCTIONS[page]()
            time_taken = format_timedelta(datetime.now() - start)
            production_logger.info(f"Success {page=} {time_taken=}")
        else:
            start = datetime.now()
            production_logger.info(f"Attempting {page} {autopool.name}")
            CONTENT_FUNCTIONS[page](autopool)
            time_taken = format_timedelta(datetime.now() - start)
            production_logger.info(f"Success {page=} {autopool.name=} {time_taken=}")
    else:
        st.text("Populating database for the first time...")


if __name__ == "__main__":
    main()
