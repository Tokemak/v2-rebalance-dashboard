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
from mainnet_launch.data_fetching.alchemy.get_events import fetch_events

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

from mainnet_launch.database.schema.track_last_processed_block_helper import (
    get_last_processed_block_for_table,
    write_last_processed_block,
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


def fetch_new_balance_updated_events(start_block: int, end_block: int, chain: ChainData) -> pd.DataFrame:
    addresses = [addr for addr in [LIQUIDATION_ROW(chain), LIQUIDATION_ROW2(chain)] if addr != DEAD_ADDRESS]

    dfs = []

    for abi in [MINIMAL_BALANCE_UPDATED_ABI_WITH_EXPECTED, MINIMAL_BALANCE_UPDATED_ABI_ONLY_BALANCE]:
        contract = chain.client.eth.contract(address=addresses[0], abi=json.loads(abi))

        df = fetch_events(
            event=contract.events.BalanceUpdated,
            chain=chain,
            start_block=start_block,
            end_block=end_block,
            addresses=addresses,
        )

        if df.empty:
            continue

        df["chain_id"] = chain.chain_id
        df["liquidation_row"] = df["address"]

        df["token"] = df["token"].apply(Web3.toChecksumAddress)
        df["vault"] = df["vault"].apply(Web3.toChecksumAddress)

        if "actualBalance" in df.columns and "balance" not in df.columns:
            # this makes the abi variants compatible inside of the db
            df = df.rename(columns={"actualBalance": "balance"})

        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def ensure_incentive_token_balance_updated_is_current() -> pd.DataFrame:
    highest_block_already_fetched = get_last_processed_block_for_table(IncentiveTokenBalanceUpdated)
    all_destinations = get_full_table_as_df(Destinations)
    valid_vaults = set(all_destinations["destination_vault_address"].tolist())

    for chain in ALL_CHAINS:
        top_block = chain.get_block_near_top()
        new_balance_updated_df = fetch_new_balance_updated_events(
            start_block=highest_block_already_fetched[chain.chain_id], chain=chain, end_block=top_block
        )

        if new_balance_updated_df.empty:
            print(f"No new IncentiveTokenBalanceUpdated events for chain {chain.name}")
            write_last_processed_block(chain, top_block, IncentiveTokenBalanceUpdated)
            continue

        ensure_all_tokens_are_saved_in_db(
            token_addresses=set(new_balance_updated_df["token"].unique().tolist()),
            chain=chain,
        )
        # make sure that we save to the tokens table any newly added incentive tokens
        token_to_decimals, token_to_symbol = get_token_details_dict()

        # don't break on on unregistered vaults, eg for new autopools
        # we add deploy destination contracts before the autopool starts, so ignore for now
        new_balance_updated_df = new_balance_updated_df[new_balance_updated_df["vault"].isin(valid_vaults)].copy()

        if new_balance_updated_df.empty:
            write_last_processed_block(chain, top_block, IncentiveTokenBalanceUpdated)
            continue
        else:

            new_balance_updated_df["new_balance"] = new_balance_updated_df.apply(
                lambda row: int(row["balance"]) / (10 ** token_to_decimals[row["token"]]), axis=1
            )
            # useful for debugging
            new_balance_updated_df["token_symbol"] = new_balance_updated_df.apply(
                lambda row: token_to_symbol[row["token"]], axis=1
            )

            new_claim_vault_rewards_rows = new_balance_updated_df.apply(
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

            ensure_all_transactions_are_saved_in_db(
                tx_hashes=new_balance_updated_df["hash"].unique().tolist(),
                chain=chain,
            )

            insert_avoid_conflicts(
                new_claim_vault_rewards_rows,
                IncentiveTokenBalanceUpdated,
            )
            print(
                f"Inserted {len(new_claim_vault_rewards_rows):,} new IncentiveTokenBalanceUpdated rows for chain {chain.name}"
            )
            write_last_processed_block(chain, top_block, IncentiveTokenBalanceUpdated)


if __name__ == "__main__":
    from mainnet_launch.constants import profile_function

    profile_function(ensure_incentive_token_balance_updated_is_current)
    # ensure_incentive_token_balance_updated_is_current()
