"""
Send a message when we expect that rewards should have been claimed from a vault, but they were not in the last 2 days.

We need to look at LiqudationVault.BalanceUpdated events to see when rewards were claimed. Other methods are not reliable.
Eg morpho vaults don't emit the same events as other typical liquidity pool vaults.

This tracks incentives in the liqudation valut itself.

We expect a reward to be claimed if 

- We have > 1 owned shares of the pool any time in the last 2 days

AND

- The incentive APR for the pool is > .5% any time in the last 2 days

Aggregation is done by pool because there are multiple vaults for the the same destination

"""

import pandas as pd

from mainnet_launch.database.postgres_operations import (
    _exec_sql_and_cache,
    get_full_table_as_df_with_tx_hash,
)

from mainnet_launch.database.views import fetch_rich_autopool_destinations_table
from mainnet_launch.database.schema.full import *
from mainnet_launch.slack_messages.post_message import post_slack_message, post_message_with_table, SlackChannel
from mainnet_launch.constants import *


N_DAYS_BALANCE_UPDATED_LOOKBACK = 2


def fetch_recent_balance_updated_events(n: int) -> pd.DataFrame:
    n_days_ago = pd.Timestamp.now() - pd.Timedelta(days=n)
    balance_updated_events = get_full_table_as_df_with_tx_hash(
        IncentiveTokenBalanceUpdated, where_clause=Blocks.datetime > n_days_ago
    ).reset_index()

    get_recent_autopool_destination_states_query = f"""
              SELECT
                  autopool_destination_states.*,
                  destinations.pool as pool,
                  blocks.datetime
              FROM autopool_destination_states
              JOIN blocks
                ON autopool_destination_states.block = blocks.block
              AND autopool_destination_states.chain_id = blocks.chain_id
              JOIN destinations
                ON autopool_destination_states.destination_vault_address = destinations.destination_vault_address
                AND autopool_destination_states.chain_id = destinations.chain_id

              WHERE blocks.datetime > '{n_days_ago}'
              
              AND
              autopool_destination_states.owned_shares > 0

              ORDER BY blocks.datetime DESC
    """

    get_recent_destination_states_query = f"""

          SELECT
          destination_states.*,
          destinations.pool as pool,
          blocks.datetime
          FROM destination_states
          JOIN blocks
          ON destination_states.block = blocks.block
          AND destination_states.chain_id = blocks.chain_id

          JOIN destinations
          ON destination_states.destination_vault_address = destinations.destination_vault_address
          AND destination_states.chain_id = destinations.chain_id

          WHERE blocks.datetime > '{n_days_ago}'

          AND 
          destination_states.incentive_apr > 0.005
          ORDER BY blocks.datetime DESC           

    """

    destinations_states_df = _exec_sql_and_cache(get_recent_destination_states_query)
    destinations_with_some_incentive_apr = destinations_states_df["destination_vault_address"].unique().tolist()
    autopool_states_df = _exec_sql_and_cache(get_recent_autopool_destination_states_query)
    destinations_with_some_owned_shares = autopool_states_df["destination_vault_address"].unique().tolist()

    destinations_with_some_expected_claims = [
        d for d in destinations_with_some_incentive_apr if d in destinations_with_some_owned_shares
    ]

    autopool_destinations = fetch_rich_autopool_destinations_table()

    highest_recent_incentive_apr = (destinations_states_df.groupby("pool")["incentive_apr"].max() * 100).to_dict()

    autopool_destinations["highest_recent_incentive_apr"] = autopool_destinations["pool"].map(
        highest_recent_incentive_apr
    )

    highest_recent_owned_shares = (autopool_states_df.groupby("pool")["owned_shares"].max()).to_dict()
    autopool_destinations["highest_recent_owned_shares"] = autopool_destinations["pool"].map(
        highest_recent_owned_shares
    )

    most_recent_claims_by_destination = (
        balance_updated_events.groupby("destination_vault_address")["datetime"].max().to_dict()
    )

    autopool_destinations["most_recent_claim"] = autopool_destinations["destination_vault_address"].map(
        most_recent_claims_by_destination
    )

    autopool_destinations["days_since_last_claim"] = (
        pd.Timestamp.now(tz="UTC") - autopool_destinations["most_recent_claim"]
    ).dt.total_seconds() / 86_400

    return autopool_destinations, destinations_with_some_expected_claims


def post_missing_balance_updated_events():
    autopool_destinations, destinations_with_some_expected_claims = fetch_recent_balance_updated_events(
        n=N_DAYS_BALANCE_UPDATED_LOOKBACK
    )

    autopool_destinations_without_expected_claims = autopool_destinations[
        autopool_destinations["destination_vault_address"].isin(destinations_with_some_expected_claims)
        & autopool_destinations["days_since_last_claim"]
        > N_DAYS_BALANCE_UPDATED_LOOKBACK
    ]

    if autopool_destinations_without_expected_claims.empty:
        post_slack_message(
            SlackChannel.PRODUCTION,
            f"All Destinations that are expected to have claimed rewards in the last {N_DAYS_BALANCE_UPDATED_LOOKBACK} days have done so.",
        )
    else:

        post_message_with_table(
            SlackChannel.PRODUCTION,
            df=autopool_destinations_without_expected_claims,
            file_save_name="Vaults Missing Expected Reward Claims.csv",
            initial_comment="Vaults Missing Expected Reward Claims",
        )


if __name__ == "__main__":
    post_missing_balance_updated_events()
