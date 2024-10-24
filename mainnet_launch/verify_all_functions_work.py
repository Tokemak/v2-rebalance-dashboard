import time
import logging

from mainnet_launch.autopool_diagnostics.autopool_diagnostics_tab import (
    fetch_autopool_diagnostics_data,
)


from mainnet_launch.autopool_diagnostics.destination_allocation_over_time import (
    fetch_destination_allocation_over_time_data,
)
from mainnet_launch.destination_diagnostics.weighted_crm import (
    fetch_weighted_crm_data,
)

from mainnet_launch.solver_diagnostics.rebalance_events import (
    fetch_rebalance_events_data,
)
from mainnet_launch.solver_diagnostics.solver_diagnostics import (
    fetch_solver_diagnostics_data,
)

from mainnet_launch.top_level.key_metrics import fetch_key_metrics_data

from mainnet_launch.constants import (
    ALL_AUTOPOOLS,
)

TESTING_LOG_FILE = "test_fetching_functions.log"

logging.basicConfig(filename=TESTING_LOG_FILE, filemode="w", format="%(asctime)s - %(message)s", level=logging.INFO)


data_caching_functions = [
    fetch_solver_diagnostics_data,
    fetch_key_metrics_data,
    fetch_autopool_diagnostics_data,
    fetch_destination_allocation_over_time_data,
    fetch_weighted_crm_data,
    fetch_rebalance_events_data,
]


def verify_data_fetching_functions_work():
    all_caching_started = time.time()
    for autopool in ALL_AUTOPOOLS:
        autopool_start_time = time.time()
        for func in data_caching_functions:
            error = None
            try:
                function_start_time = time.time()
                func(autopool)
                time_taken = time.time() - function_start_time
                logging.info(f"{time_taken:06.2f} \t seconds: Cached {func.__name__}({autopool.name})")
            except Exception as error:
                time_taken = time.time() - function_start_time
                logging.info(f"{time_taken:06.2f} \t seconds: ERROR {func.__name__}({autopool.name})")
                logging.error(f"Exception occurred: {str(error)}")
                logging.error("Stack Trace:", exc_info=True)

        autopool_time_taken = time.time() - autopool_start_time
        logging.info(f"{autopool_time_taken:06.2f} \t seconds: Cached {autopool.name}")

    all_autopool_time_taken = time.time() - all_caching_started
    logging.info(f"{all_autopool_time_taken:06.2f} \t seconds: All Autopools Cached")
    logging.info(f"Finished Caching")
    with open(TESTING_LOG_FILE, "r") as log_file:
        log_contents = log_file.read()

    if "Exception occurred:" in log_contents:
        print(log_contents)


if __name__ == "__main__":
    verify_data_fetching_functions_work()
