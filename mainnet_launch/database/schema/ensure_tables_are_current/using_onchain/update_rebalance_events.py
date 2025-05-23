import json
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from mainnet_launch.database.schema.full import (
    RebalancePlans,
    RebalanceEvents,
    Destinations,
    Autopools,
    RebalanceEvents,
)

from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    natural_left_right_using_where,
    get_highest_value_in_field_where,
)

from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, ALL_CHAINS


def subgraph_load_rebalance_events():
    pass


def ensure_rebalance_events_are_current():
    # s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    autopools: list[Autopools] = get_full_table_as_orm(Autopools)

    for autopool in autopools:

        get_subset_not_already_in_column(
            RebalanceEvents,
            RebalanceEvents.tx_hash,
            [],
            where_clause=RebalanceEvents.autopool_vault_address == autopool.autopool_vault_address,
        )

        pass

        # insert_avoid_conflicts(all_rebalance_plan_rows, RebalancePlan, index_elements=[RebalancePlan.file_name])
        # insert_avoid_conflicts(
        #     all_dex_steps_rows, DexSwapSteps, index_elements=[DexSwapSteps.file_name, DexSwapSteps.step_index]
        # )


if __name__ == "__main__":
    ensure_rebalance_events_are_current()
