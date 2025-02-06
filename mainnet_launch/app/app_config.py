import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

STREAMLIT_IN_MEMORY_CACHE_TIME = int(os.environ["STREAMLIT_IN_MEMORY_CACHE_TIME"])

SHOULD_UPDATE_DATABASE_MAX_LATENCY = pd.Timedelta(os.environ["SHOULD_UPDATE_DATABASE_MAX_LATENCY"])

NUM_GAS_INFO_FETCHING_THREADS = int(os.environ["NUM_GAS_INFO_FETCHING_THREADS"])

SEMAPHORE_LIMITS_FOR_MULTICALL = tuple(int(x) for x in os.environ["SEMAPHORE_LIMITS_FOR_MULTICALL"].split(","))
