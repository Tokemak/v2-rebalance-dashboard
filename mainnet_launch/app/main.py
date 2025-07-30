from mainnet_launch.app.ui_config_setup import config_plotly_and_streamlit, STREAMLIT_MARKDOWN_HTML, format_timedelta

config_plotly_and_streamlit()

from datetime import datetime
import streamlit as st
import logging


from mainnet_launch.constants import ALL_AUTOPOOLS
from mainnet_launch.pages.page_functions import (
    AUTOPOOL_CONTENT_FUNCTIONS,
    PROTOCOL_CONTENT_FUNCTIONS,
    CHAIN_SPECIFIC_FUNCTIONS,
)


from mainnet_launch.app.ui_config_setup import config_plotly_and_streamlit, STREAMLIT_MARKDOWN_HTML, format_timedelta
from mainnet_launch.constants import ALL_AUTOPOOLS
from mainnet_launch.pages.page_functions import (
    PROTOCOL_CONTENT_FUNCTIONS,
    CHAIN_SPECIFIC_FUNCTIONS,
    AUTOPOOL_CONTENT_FUNCTIONS,
)
import streamlit as st
from datetime import datetime


def render_see_options():
    st.write("### Protocol-wide pages")
    for name in PROTOCOL_CONTENT_FUNCTIONS.keys():
        st.write(f"- {name}")

    st.write("### Chain-wide pages")
    for name in CHAIN_SPECIFIC_FUNCTIONS.keys():
        st.write(f"- {name}")

    st.write("### Autopool specific pages ")
    for name in AUTOPOOL_CONTENT_FUNCTIONS.keys():
        st.write(f"- {name}")


def main():
    st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
    st.title("Autopool Diagnostics Dashboard")

    # 1) Choose category
    category = st.sidebar.radio("Page type", ("See Options", "Protocol-wide", "Chain-wide", "Autopool"), index=0)

    if category == "Pages":
        render_see_options()
        return

    page = None

    if category == "Protocol-wide":
        page = st.sidebar.radio("Protocol Pages", list(PROTOCOL_CONTENT_FUNCTIONS.keys()))

    elif category == "Chain-wide":
        page = st.sidebar.radio("Chain Pages", list(CHAIN_SPECIFIC_FUNCTIONS.keys()))

    else:  # Autopool
        autopool = None
        names = [a.name for a in ALL_AUTOPOOLS]
        selection = st.sidebar.radio("Select Autopool", names)
        autopool = {a.name: a for a in ALL_AUTOPOOLS}[selection]

        # then show all autopool‑specific pages
        page = st.sidebar.radio("Autopool Pages", list(AUTOPOOL_CONTENT_FUNCTIONS.keys()))

    # 3) Render exactly one
    if page:
        if category == "Protocol-wide":
            PROTOCOL_CONTENT_FUNCTIONS[page]()

        elif category == "Chain-wide":
            CHAIN_SPECIFIC_FUNCTIONS[page]()

        elif category == "Autopool":
            AUTOPOOL_CONTENT_FUNCTIONS[page](autopool)


if __name__ == "__main__":
    main()


# from mainnet_launch.app.ui_config_setup import config_plotly_and_streamlit, STREAMLIT_MARKDOWN_HTML, format_timedelta

# config_plotly_and_streamlit()

# from datetime import datetime
# import streamlit as st
# import logging


# from mainnet_launch.constants import ALL_AUTOPOOLS, PRODUCTION_LOG_FILE_NAME
# from mainnet_launch.pages.page_functions import CONTENT_FUNCTIONS, PAGES_WITHOUT_AUTOPOOL


# production_logger = logging.getLogger("production_logger")
# production_logger.setLevel(logging.INFO)

# # Only add the handler if it doesn't already exist
# if not production_logger.hasHandlers():
#     handler = logging.FileHandler(PRODUCTION_LOG_FILE_NAME, mode="w")
#     handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
#     production_logger.addHandler(handler)
#     production_logger.propagate = False


# def main():
#     st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
#     st.title("Autopool Diagnostics Dashboard")
#     st.sidebar.title("Navigation")

#     names = [autopool.name for autopool in ALL_AUTOPOOLS]
#     pool_name = st.sidebar.selectbox("Select Pool", names)
#     autopool_name_to_constants = {a.name: a for a in ALL_AUTOPOOLS}
#     autopool = autopool_name_to_constants[pool_name]

#     page = st.sidebar.radio("Go to", CONTENT_FUNCTIONS.keys())

#     if page in PAGES_WITHOUT_AUTOPOOL:
#         start = datetime.now()
#         CONTENT_FUNCTIONS[page]()
#         time_taken = format_timedelta(datetime.now() - start)
#         production_logger.info(f"Success {page=} {time_taken=}")
#     else:
#         start = datetime.now()
#         CONTENT_FUNCTIONS[page](autopool)
#         time_taken = format_timedelta(datetime.now() - start)
#         production_logger.info(f"Success {page=} {autopool.name=} {time_taken=}")


# if __name__ == "__main__":
#     main()
