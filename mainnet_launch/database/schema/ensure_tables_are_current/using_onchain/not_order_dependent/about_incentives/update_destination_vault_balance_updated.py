import pandas as pd
from web3 import Web3


from mainnet_launch.abis import TOKEMAK_LIQUIDATION_ROW_ABI
from mainnet_launch.constants import (
    ChainData,
    LIQUIDATION_ROW,
    LIQUIDATION_ROW2,
    ALL_CHAINS,
    PLASMA_CHAIN,
    DEAD_ADDRESS,
)

import json
from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.database.views import get_token_details_dict
from mainnet_launch.database.schema.full import IncentiveTokenBalanceUpdated, Destinations
from mainnet_launch.database.postgres_operations import (
    _exec_sql_and_cache,
    insert_avoid_conflicts,
    get_full_table_as_df,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_destinations_tokens_and_autopoolDestinations_table import (
    ensure_all_tokens_are_saved_in_db,
)

# used by plasma and some of the other new chains
MINIMAL_BALANCE_UPDATED_ABI_WITH_EXPECTED = """[
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true,  "internalType": "address", "name": "token",           "type": "address" },
      { "indexed": true,  "internalType": "address", "name": "vault",           "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "actualBalance",   "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "expectedBalance", "type": "uint256" }
    ],
    "name": "BalanceUpdated",
    "type": "event"
  }
]"""

# used by eth and base
MINIMAL_BALANCE_UPDATED_ABI_ONLY_BALANCE = """[
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true,  "internalType": "address", "name": "token",  "type": "address" },
      { "indexed": true,  "internalType": "address", "name": "vault",  "type": "address" },
      { "indexed": false, "internalType": "uint256", "name": "balance", "type": "uint256" }
    ],
    "name": "BalanceUpdated",
    "type": "event"
  }
]
"""


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
    dfs = []
    for liqudation_row in [LIQUIDATION_ROW, LIQUIDATION_ROW2]:
        for abi in [MINIMAL_BALANCE_UPDATED_ABI_WITH_EXPECTED, MINIMAL_BALANCE_UPDATED_ABI_ONLY_BALANCE]:
            if liqudation_row(chain) == DEAD_ADDRESS:
                continue
        contract = chain.client.eth.contract(address=liqudation_row(chain), abi=json.loads(abi))
        balance_updated_df = fetch_events(
            event=contract.events.BalanceUpdated,
            chain=chain,
            start_block=start_block,
        )
        balance_updated_df["chain_id"] = chain.chain_id
        balance_updated_df["token"] = balance_updated_df["token"].apply(lambda x: Web3.toChecksumAddress(x))
        balance_updated_df["vault"] = balance_updated_df["vault"].apply(lambda x: Web3.toChecksumAddress(x))

        if "actualBalance" in balance_updated_df.columns:
            balance_updated_df = balance_updated_df.rename(columns={"actualBalance": "balance"})

        balance_updated_df["liquidation_row"] = liqudation_row(chain)
        dfs.append(balance_updated_df)

    all_balance_updated_df = pd.concat(dfs, axis=0)
    return all_balance_updated_df


def ensure_incentive_token_balance_updated_is_current() -> pd.DataFrame:
    highest_block_already_fetched = _get_highest_block_already_fetched_by_chain_id()
    all_destinations = get_full_table_as_df(Destinations)
    valid_vaults = set(all_destinations["destination_vault_address"].tolist())

    for target_chain in ALL_CHAINS:
        all_balance_updated_df = fetch_new_balance_updated_events(
            start_block=highest_block_already_fetched.get(
                target_chain.chain_id, target_chain.block_autopool_first_deployed
            )
            + 1,
            chain=target_chain,
        )

        ensure_all_tokens_are_saved_in_db(
            token_addresses=set(all_balance_updated_df["token"].unique().tolist()),
            chain=target_chain,
        )
        # make sure that we save to the tokens table any newly added incentive tokens
        token_to_decimals, token_to_symbol = get_token_details_dict()
        # don't break on on unregistered vaults, eg for new autopools
        # we add deploy destination contracts before the autopool starts, so ignore for now

        all_balance_updated_df = all_balance_updated_df[all_balance_updated_df["vault"].isin(valid_vaults)].copy()

        if all_balance_updated_df.empty:
            continue
        else:

            all_balance_updated_df["new_balance"] = all_balance_updated_df.apply(
                lambda row: int(row["balance"]) / (10 ** token_to_decimals[row["token"]]), axis=1
            )
            # useful for debugging
            all_balance_updated_df["token_symbol"] = all_balance_updated_df.apply(
                lambda row: token_to_symbol[row["token"]], axis=1
            )

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
    from mainnet_launch.constants import profile_function

    # profile_function(ensure_incentive_token_balance_updated_is_current)
    ensure_incentive_token_balance_updated_is_current()
