from mainnet_launch.ui_config_setup import config_plotly_and_streamlit

config_plotly_and_streamlit()

import streamlit as st
import logging
import psutil

from mainnet_launch.constants import ALL_AUTOPOOLS
from mainnet_launch.page_functions import (
    CONTENT_FUNCTIONS,
    PAGES_WITHOUT_AUTOPOOL,
)

from mainnet_launch.constants import (
    ALL_AUTOPOOLS,
)


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


production_logger = logging.getLogger("testing_logger")
production_logger.setLevel(logging.INFO)

# Only add the handler if it doesn't already exist
if not production_logger.hasHandlers():
    handler = logging.FileHandler("data_caching.log", mode="w")
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    production_logger.addHandler(handler)
    production_logger.propagate = False


def main():
    st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
    st.title("Autopool Diagnostics Dashboard")
    st.sidebar.title("Navigation")

    names = [autopool.name for autopool in ALL_AUTOPOOLS]
    pool_name = st.sidebar.selectbox("Select Pool", names)
    autopool_name_to_constants = {a.name: a for a in ALL_AUTOPOOLS}
    autopool = autopool_name_to_constants[pool_name]

    page = st.sidebar.radio("Go to", CONTENT_FUNCTIONS.keys())

    if page in PAGES_WITHOUT_AUTOPOOL:
        CONTENT_FUNCTIONS[page]()
    else:
        CONTENT_FUNCTIONS[page](autopool)


if __name__ == "__main__":
    main()


# from mainnet_launch.ui_config_setup import config_plotly_and_streamlit

# config_plotly_and_streamlit()

# import streamlit as st
# import atexit
# import threading
# import time
# import logging
# import os
# import psutil

# from mainnet_launch.constants import ALL_AUTOPOOLS, ROOT_DIR
# from mainnet_launch.page_functions import (
#     CONTENT_FUNCTIONS,
#     PAGES_WITHOUT_AUTOPOOL,
#     NOT_PER_AUTOPOOL_DATA_CACHING_FUNCTIONS,
#     PER_AUTOPOOOL_DATA_CACHING_FUNCTIONS,
# )

# from mainnet_launch.constants import (
#     CACHE_TIME,
#     ALL_AUTOPOOLS,
# )


# STREAMLIT_MARKDOWN_HTML = """
#         <style>
#         .main {
#             max-width: 85%;
#             margin: 0 auto;
#             padding-top: 40px;
#         }
#         .stPlotlyChart {
#             width: 100%;
#             height: auto;
#             min-height: 300px;
#             max-height: 600px;
#             background-color: #f0f2f6;
#             border-radius: 5px;
#             padding: 20px;
#         }
#         @media (max-width: 768px) {
#             .stPlotlyChart {
#                 min-height: 250px;
#                 max-height: 450px;
#             }
#         }
#         .stPlotlyChart {
#             background-color: #f0f2f6;
#             border-radius: 5px;
#             padding: 10px;
#         }
#         .stExpander {
#             background-color: #e6e9ef;
#             border-radius: 5px;
#             padding: 10px;
#         }
#         </style>
#         """


# def get_memory_usage():
#     process = psutil.Process()
#     mem_info = process.memory_info()
#     return mem_info.rss / (1024**2)


# production_logger = logging.getLogger("testing_logger")
# production_logger.setLevel(logging.INFO)

# # Only add the handler if it doesn't already exist
# if not production_logger.hasHandlers():
#     handler = logging.FileHandler("data_caching.log", mode="w")
#     handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
#     production_logger.addHandler(handler)
#     production_logger.propagate = False


# cache_file_lock_check = ROOT_DIR / "cache_thread_already_running.lock"


# def cleanup():
#     if os.path.exists(cache_file_lock_check):
#         os.remove(cache_file_lock_check)
#         production_logger.info("cache_file_lock_check removed on program exit.")


# atexit.register(cleanup)


# def log_and_time_function(page_name, func, autopool):
#     start_time = time.time()
#     error = None
#     if autopool is None:
#         try:
#             func()
#         except Exception as e:
#             error = e
#     else:
#         try:
#             func(autopool)
#         except Exception as e:
#             error = e
#     time_taken = time.time() - start_time
#     autopool_name = None
#     if autopool is not None:
#         autopool_name = autopool.name
#     if error is not None:
#         production_logger.info(
#             f"Error: {time_taken:.2f} seconds | {func.__name__} |  {page_name} {autopool_name} {error}"
#         )
#     else:
#         production_logger.info(
#             f"Success: {time_taken:.2f} seconds | {func.__name__} |  {page_name} {autopool_name} {error}"
#         )


# def _cache_autopool_data():
#     all_caching_started = time.time()
#     production_logger.info("Start Autopool Functions")
#     for func in PER_AUTOPOOOL_DATA_CACHING_FUNCTIONS:
#         for autopool in ALL_AUTOPOOLS:
#             log_and_time_function("caching", func, autopool)

#     production_logger.info(f"{time.time() - all_caching_started:.2f} seconds: All Autopools Cached")


# def _cache_network_data():
#     production_logger.info("Start Network Functions")
#     network_start_time = time.time()
#     for func in NOT_PER_AUTOPOOL_DATA_CACHING_FUNCTIONS:
#         log_and_time_function("caching thread", func, None)
#     production_logger.info(f"{time.time() - network_start_time:.2f} seconds: Cached Network Functions")


# def _cache_data():
#     all_caching_started = time.time()
#     _cache_autopool_data()
#     _cache_network_data()
#     production_logger.info(f"{time.time() - all_caching_started:.2f} seconds: Everything Cached")
#     production_logger.info("Finished Caching, Starting Sleep")


# def cache_data_loop():
#     production_logger.info("Started cache_data_loop()")
#     try:
#         while True:
#             _cache_data()
#             time.sleep(CACHE_TIME + (60 * 5))  # + 5 minutes
#             production_logger.info("Finished Sleeping")
#     except Exception as e:
#         production_logger.exception(str(e))
#         production_logger.info(f"Cache data loop ended at {time.strftime('%Y-%m-%d %H:%M:%S')}")
#         raise e
#     finally:
#         # Clean up the lock file
#         if os.path.exists(cache_file_lock_check):
#             os.remove(cache_file_lock_check)
#             production_logger.info("cache_file_lock_check removed on thread exit.")


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
#         CONTENT_FUNCTIONS[page]()
#     else:
#         CONTENT_FUNCTIONS[page](autopool)

#     if st.button("Start Another Cache Thread"):
#         # if st.text_input("""write: I know what I'm doing """) == "I know what I'm doing":
#         if os.path.exists(cache_file_lock_check):
#             os.remove(cache_file_lock_check)
#             production_logger.info("cache_file_lock_check removed by user action.")
#             st.success("Cache lock file deleted successfully.")
#             start_cache_thread()
#             st.success("Started another cache thread")
#             # there are very low odds of having multiple threads going at once,
#             # this will add in another thread that will call the caching functions
#             # the prior setup reacreated a caching thread with every new users sesssion
#             # I don't expect to need to press this button but it is nice to have for if
#             # some unknown, (power loss, KILL-9) ends the program without deleting the lock file
#             # this manually starts another caching thread


# def start_cache_thread():
#     if os.path.exists(cache_file_lock_check):
#         production_logger.info(f"Not starting another thread because {cache_file_lock_check} already exists")
#     else:
#         production_logger.info(f"Starting First thread because {cache_file_lock_check} does not exist")
#         with open(cache_file_lock_check, "w") as fout:
#             pass  # just open the file and don't write anything to it

#         fetch_thread = threading.Thread(target=cache_data_loop, daemon=True)
#         fetch_thread.start()  # this thread should keep going as long as the program is running


# if __name__ == "__main__":
#     # start_cache_thread()
#     main()
