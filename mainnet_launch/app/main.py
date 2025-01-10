from mainnet_launch.app.ui_config_setup import config_plotly_and_streamlit, STREAMLIT_MARKDOWN_HTML, format_timedelta

config_plotly_and_streamlit()

from datetime import datetime
import streamlit as st
import logging
import os


from mainnet_launch.constants import ALL_AUTOPOOLS, PRODUCTION_LOG_FILE_NAME, ROOT_DIR
from mainnet_launch.pages.page_functions import (
    CONTENT_FUNCTIONS,
    PAGES_WITHOUT_AUTOPOOL,
)
from mainnet_launch.app.run_on_startup import first_run_of_db


production_logger = logging.getLogger("production_logger")
production_logger.setLevel(logging.INFO)

# Only add the handler if it doesn't already exist
if not production_logger.hasHandlers():
    handler = logging.FileHandler(PRODUCTION_LOG_FILE_NAME, mode="w")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    production_logger.addHandler(handler)
    production_logger.propagate = False


FINISHED_STARTUP_FILE = ROOT_DIR / "app/finished_startup.txt"


def render_ui():
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
        CONTENT_FUNCTIONS[page]()
        time_taken = format_timedelta(datetime.now() - start)
        production_logger.info(f"Success {page=} {time_taken=}")
    else:
        start = datetime.now()
        CONTENT_FUNCTIONS[page](autopool)
        time_taken = format_timedelta(datetime.now() - start)
        production_logger.info(f"Success {page=} {autopool.name=} {time_taken=}")


def main():
    if not os.path.exists(FINISHED_STARTUP_FILE):
        st.title("Startup Process")
        st.warning(
            "Keep this tab open and don't refresh, or open new tabs to this page after clicking start. Take ~15 minutes."
        )

        if st.button("Start Startup Process"):
            first_run_of_db(production_logger)

            with open(FINISHED_STARTUP_FILE, "x") as _:
                pass
            st.text("Finished startup, refresh to use app")

        try:
            with open(PRODUCTION_LOG_FILE_NAME, "r") as log_file:
                log_contents = log_file.read()
            st.text_area("Production Log", log_contents, height=300)
        except FileNotFoundError:
            st.text("Log file not found yet.")
    else:
        render_ui()


if __name__ == "__main__":
    main()
