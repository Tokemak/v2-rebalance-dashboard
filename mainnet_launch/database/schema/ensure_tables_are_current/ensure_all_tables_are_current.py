"""

Top line scirpt that updates the database to the current time
Run this, via a once a day lambada function (or as needed) to update the dashboard pulling from the db

"""

from mainnet_launch.constants import time_decorator


from mainnet_launch.database.schema.ensure_tables_are_current.update_destinations_table import (
    ensure_destinations_are_current,
)
from mainnet_launch.database.schema.ensure_tables_are_current.update_autopools_table import ensure_autopools_is_current
from mainnet_launch.database.schema.full import drop_and_full_rebuild_db
from mainnet_launch.database.schema.ensure_tables_are_current.update_token_values_table import (
    ensure_token_values_are_current,
)


from mainnet_launch.data_fetching.block_timestamp import ensure_blocks_is_current


# this took ensure_database_is_current took 732.4539 seconds. from 0
# the second one took 5 seconds
@time_decorator
def ensure_database_is_current(full_reset_and_refetch: bool = False):
    if full_reset_and_refetch:
        drop_and_full_rebuild_db()
    # not sure on this part why it takes so long
    ensure_blocks_is_current()  # this is the part that takes forever 750 seconds (only sometimes)
    ensure_destinations_are_current()
    ensure_autopools_is_current()
    ensure_token_values_are_current()
    # add destination token values
    # add destiantion states
    # add autopool destiation states
    # add autopool destiations tates


if __name__ == "__main__":
    ensure_database_is_current(True)
    ensure_database_is_current(False)
