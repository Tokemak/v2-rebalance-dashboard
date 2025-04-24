"""

Top line scirpt that updates the database to the current time
Run this, via a once a day lambada function (or as needed) to update the dashboard pulling from the db

"""

from mainnet_launch.constants import time_decorator


from mainnet_launch.database.schema.ensure_tables_are_current.update_destinations_table import (
    ensure_destinations_are_current,
)
from mainnet_launch.database.schema.ensure_tables_are_current.update_autopools_table import ensure_autopools_is_current


from mainnet_launch.data_fetching.block_timestamp import ensure_blocks_is_current


@time_decorator
def ensure_database_is_current():
    ensure_blocks_is_current()
    ensure_destinations_are_current()
    ensure_autopools_is_current()


if __name__ == "__main__":
    ensure_database_is_current()
