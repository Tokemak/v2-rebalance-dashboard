from concurrent.futures import ThreadPoolExecutor, as_completed


import pandas as pd
from web3 import Web3


from mainnet_launch.abis import TOKEMAK_LIQUIDATION_ROW_ABI
from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.database.views import get_token_details_dict
from mainnet_launch.database.schema.full import IncentiveTokenBalanceUpdated
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache, insert_avoid_conflicts

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_destinations_tokens_and_autopoolDestinations_table import (
    ensure_all_tokens_are_saved_in_db,
)

from mainnet_launch.constants import ChainData, LIQUIDATION_ROW, LIQUIDATION_ROW2, ALL_CHAINS


def _get_highest_block_already_fetched_by_chain_id() -> dict[int, int]:
    query = """
        SELECT
            incentive_token_balance_updated.chain_id,
            MAX(transactions.block) AS max_block
        FROM incentive_token_balance_updated
        JOIN transactions
            ON transactions.tx_hash = incentive_token_balance_updated.tx_hash
        AND transactions.chain_id = incentive_token_balance_updated.chain_id
        GROUP BY
            incentive_token_balance_updated.chain_id
"""
    highest_block_fetched_by_chain = _exec_sql_and_cache(query)

    if highest_block_fetched_by_chain.empty:
        highest_block_already_fetched = dict()
    else:
        highest_block_already_fetched = highest_block_fetched_by_chain.set_index("chain_id")["max_block"].to_dict()

    return highest_block_already_fetched


def fetch_new_balance_updated_events(
    start_block: int,
    chain: ChainData,
) -> pd.DataFrame:

    all_balance_updated = []

    liquidation_row1_contract = chain.client.eth.contract(
        address=LIQUIDATION_ROW(chain), abi=TOKEMAK_LIQUIDATION_ROW_ABI
    )

    liquidation_row2_contract = chain.client.eth.contract(
        address=LIQUIDATION_ROW2(chain), abi=TOKEMAK_LIQUIDATION_ROW_ABI
    )

    balanceUpdated_1: pd.DataFrame = fetch_events(
        event=liquidation_row1_contract.events.BalanceUpdated,
        chain=chain,
        start_block=start_block,
    )
    balanceUpdated_1["liquidation_row"] = LIQUIDATION_ROW(chain)

    balance_updated_2: pd.DataFrame = fetch_events(
        event=liquidation_row2_contract.events.BalanceUpdated,
        chain=chain,
        start_block=start_block,
    )
    balance_updated_2["liquidation_row"] = LIQUIDATION_ROW2(chain)

    all_balance_updated.extend([balanceUpdated_1, balance_updated_2])

    all_balance_updated_df = pd.concat(all_balance_updated, ignore_index=True)
    all_balance_updated_df["chain_id"] = chain.chain_id

    all_balance_updated_df["token"] = all_balance_updated_df["token"].apply(lambda x: Web3.toChecksumAddress(x))
    all_balance_updated_df["vault"] = all_balance_updated_df["vault"].apply(lambda x: Web3.toChecksumAddress(x))
    return all_balance_updated_df


def ensure_incentive_token_balance_updated_is_current() -> pd.DataFrame:
    highest_block_already_fetched = _get_highest_block_already_fetched_by_chain_id()

    for target_chain in ALL_CHAINS:
        all_balance_updated_df = fetch_new_balance_updated_events(
            start_block=highest_block_already_fetched.get(target_chain.chain_id, 0) + 1,
            chain=target_chain,
        )

        ensure_all_tokens_are_saved_in_db(all_balance_updated_df["token"].unique().tolist(), target_chain)

        token_to_decimals, token_to_symbol = get_token_details_dict()

        all_balance_updated_df["new_balance"] = all_balance_updated_df.apply(
            lambda row: int(row["balance"]) / (10 ** token_to_decimals[row["token"]]), axis=1
        )

        # useful for debugging
        all_balance_updated_df["token_symbol"] = all_balance_updated_df.apply(
            lambda row: token_to_symbol[row["token"]], axis=1
        )

        if all_balance_updated_df.empty:
            continue
        else:
            new_claim_vault_rewards_rows = all_balance_updated_df.apply(
                lambda row: IncentiveTokenBalanceUpdated(
                    tx_hash=row["hash"],
                    log_index=row["log_index"],
                    chain_id=row["chain_id"],
                    liquidation_row=row["liquidation_row"],
                    token_address=row["token"],
                    destination_vault_address=row["vault"],
                    new_balance=row["new_balance"],
                ),
                axis=1,
            ).tolist()

            new_hashes = [r.tx_hash for r in new_claim_vault_rewards_rows]

            ensure_all_transactions_are_saved_in_db(
                tx_hashes=new_hashes,
                chain=target_chain,
            )
            

            
            insert_avoid_conflicts(
                new_claim_vault_rewards_rows,
                IncentiveTokenBalanceUpdated,
            )


if __name__ == "__main__":
    # is for the new infinifi USD pool, ignore for now
    missing_destination_vault = '0x648Ca495d5D6f310f3DB01015d815779eFF2Fec7' # mainnet, not sure why it isn't here
    ensure_incentive_token_balance_updated_is_current()
