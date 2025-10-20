"""
Send a message when we expect that rewards should have been claimed from a vault, but they were not in the last 2 days

We expect a reward to be claimed if 

- We have > 1 owned shares of the pool any time in the last 2 days

AND

- The incentive APR for the pool is > .5% any time in the last 2 days

Aggregation is done by pool because there are multiple vaults for the the same destiantion


"""


def fetch_recent_vault_claimed_reward_event_data(n: int = 2) -> pd.DataFrame:

    chain_id_to_name = {c.chain_id: c.name for c in ALL_CHAINS}

    n_days_ago = pd.Timestamp.now() - pd.Timedelta(days=n)

    get_recent_vault_claimed_reward_events_query = f"""

                SELECT
                  claim_vault_rewards.*,
                  destinations.pool as pool,
                  blocks.datetime,
                  blocks.block
              FROM claim_vault_rewards
              JOIN transactions
                ON claim_vault_rewards.tx_hash = transactions.tx_hash
              JOIN blocks
                ON transactions.block = blocks.block
              AND transactions.chain_id = blocks.chain_id
              JOIN destinations
                ON claim_vault_rewards.destination_vault_address = destinations.destination_vault_address
                AND claim_vault_rewards.chain_id = destinations.chain_id
              
              WHERE blocks.datetime > '{n_days_ago}'
              ORDER BY blocks.datetime DESC
  """

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
          ORDER BY blocks.datetime DESC           

  """

    claim_vault_rewards = _exec_sql_and_cache(
        get_recent_vault_claimed_reward_events_query,
    )

    autopool_destination_states = _exec_sql_and_cache(
        get_recent_autopool_destination_states_query,
    )

    destination_states = _exec_sql_and_cache(
        get_recent_destination_states_query,
    )

    destinations = get_full_table_as_df(Destinations)

    # add the highest incentive APR and the most number of owned shares in the period
    destinations["incentive_apr"] = destinations["pool"].map(destination_states.groupby("pool")["incentive_apr"].max())
    destinations["owned_shares"] = destinations["pool"].map(
        autopool_destination_states.groupby("pool")["owned_shares"].max()
    )
    destinations["chain"] = destinations["chain_id"].map(chain_id_to_name)
    return claim_vault_rewards, autopool_destination_states, destination_states, destinations
