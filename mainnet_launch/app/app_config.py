"""Settings related to the app. ie rate limiting latency etc"""

import pandas as pd

# how long to keep data in the streamlit in-memory cache
STREAMLIT_IN_MEMORY_CACHE_TIME = 3600 * 6  # six hours in seconds


# only make external calls and rows to a database if > SHOULD_UPDATE_DATABASE_MAX_LATENCY
# seconds have passed since adding new rows
SHOULD_UPDATE_DATABASE_MAX_LATENCY = pd.Timedelta("6 hours")


# How many threads to use for fetching gas used and gas prices
NUM_GAS_INFO_FETCHING_THREADS = 8


# Rate limits for mulitcall.py
#  A sequence of concurrency limits. The function starts with the first (largest) limit
#  to quickly attempt all calls. If some fail, it retries them with the next (smaller)
#  limit, and so forth, providing a backoff mechanism for handling transient failures.
SEMAPHORE_LIMITS_FOR_MULTCIALL = (300, 300, 50, 20, 2)
