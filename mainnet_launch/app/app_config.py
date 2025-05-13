import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


STREAMLIT_IN_MEMORY_CACHE_TIME = int(os.environ.get("STREAMLIT_IN_MEMORY_CACHE_TIME", 21600))  # 6 hours

SHOULD_UPDATE_DATABASE_MAX_LATENCY = pd.Timedelta(os.environ.get("SHOULD_UPDATE_DATABASE_MAX_LATENCY", "6 hours"))

NUM_GAS_INFO_FETCHING_THREADS = int(os.environ.get("NUM_GAS_INFO_FETCHING_THREADS", 16))

SEMAPHORE_LIMITS_FOR_MULTICALL = tuple(
    int(x) for x in os.environ.get("SEMAPHORE_LIMITS_FOR_MULTICALL", "100,20,1").split(",")
)

NUM_S3_BUCKET_FETCHING_THREADS = int(os.environ.get("NUM_S3_BUCKET_FETCHING_THREADS", 64))
