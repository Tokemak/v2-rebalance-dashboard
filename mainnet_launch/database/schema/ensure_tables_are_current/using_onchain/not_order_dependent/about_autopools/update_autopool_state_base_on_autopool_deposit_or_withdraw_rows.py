import pandas as pd


from mainnet_launch.constants import ALL_AUTOPOOLS, profile_function
from mainnet_launch.database.schema.full import AutopoolStates
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    insert_avoid_conflicts,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_autopool_states import (
    _fetch_new_autopool_state_rows,
)


def _get_blocks_in_withdrawals_or_deposits_not_in_states() -> pd.DataFrame:
    query = """
            WITH events AS (
                SELECT w.autopool_vault_address, w.chain_id, tx.block
                FROM autopool_withdrawals AS w
                JOIN transactions AS tx
                ON tx.tx_hash = w.tx_hash

                UNION

                SELECT d.autopool_vault_address, d.chain_id, tx.block
                FROM autopool_deposits AS d
                JOIN transactions AS tx
                ON tx.tx_hash = d.tx_hash
            )
            SELECT e.autopool_vault_address, e.chain_id, e.block
            FROM events e
            LEFT JOIN autopool_states AS s
            ON s.autopool_vault_address = e.autopool_vault_address
            AND s.chain_id = e.chain_id
            AND s.block = e.block
            WHERE s.autopool_vault_address IS NULL
            ORDER BY e.chain_id, e.autopool_vault_address, e.block;
        """

    df = _exec_sql_and_cache(query)
    return df


def ensure_an_autopool_state_exists_for_each_autopool_withdrawal_or_deposit():
    missing_blocks_df = _get_blocks_in_withdrawals_or_deposits_not_in_states()

    if missing_blocks_df.empty:
        return

    autopool_vault_address_to_missing_blocks = (
        missing_blocks_df.groupby(["autopool_vault_address"])["block"].apply(list).to_dict()
    )

    for autopool in ALL_AUTOPOOLS:

        missing_blocks = autopool_vault_address_to_missing_blocks.get(autopool.autopool_eth_addr, [])

        if not missing_blocks:
            continue

        new_autopool_states_rows = _fetch_new_autopool_state_rows(autopool, missing_blocks)

        insert_avoid_conflicts(new_autopool_states_rows, AutopoolStates)
        print(f"Inserted {len(new_autopool_states_rows)} new autopool states for {autopool.autopool_eth_addr}")


if __name__ == "__main__":
    profile_function(ensure_an_autopool_state_exists_for_each_autopool_withdrawal_or_deposit)
