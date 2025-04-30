import json
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from mainnet_launch.database.schema.full import RebalancePlans, Destinations, DexSwapSteps, Autopools

from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)

from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, ALL_CHAINS


def ensure_rebalance_events_are_current():
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    autopools = get_full_table_as_orm(
        Autopools,
    )

    for chain in ALL_CHAINS:

        insert_avoid_conflicts(all_rebalance_plan_rows, RebalancePlan, index_elements=[RebalancePlan.file_name])
        insert_avoid_conflicts(
            all_dex_steps_rows, DexSwapSteps, index_elements=[DexSwapSteps.file_name, DexSwapSteps.step_index]
        )


if __name__ == "__main__":
    ensure_rebalance_plans_table_are_current()
