"""
For each autopool has the price return changed by 30bps in the last day? idk?

"""

import pandas as pd

from mainnet_launch.database.postgres_operations import (
    _exec_sql_and_cache,
    get_full_table_as_df_with_tx_hash,
    get_full_table_as_df_with_block,
    ENGINE,
    get_full_table_as_df,
)
from mainnet_launch.database.schema.full import *

from mainnet_launch.slack_messages.post_message import post_slack_message, post_message_with_table
from mainnet_launch.constants import ALL_CHAINS
