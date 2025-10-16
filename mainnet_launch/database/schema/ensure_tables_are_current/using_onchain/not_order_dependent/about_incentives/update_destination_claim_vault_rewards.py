import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.abis import MINIMAL_DESTINATION_VAULT_REWARD_CLAIMED_EVENT_ABI
from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.database.views import get_token_details_dict
from mainnet_launch.database.schema.full import ClaimVaultRewards
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache, insert_avoid_conflicts

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_destinations_tokens_and_autopoolDestinations_table import (
    ensure_all_tokens_are_saved_in_db,
)

from concurrent.futures import ThreadPoolExecutor, as_completed


def _get_highest_block_already_fetched_by_destination_vault_address() -> dict:
    query = """
        SELECT
            claim_vault_rewards.destination_vault_address,
            MAX(transactions.block) AS max_block
        FROM claim_vault_rewards
        JOIN transactions
            ON transactions.tx_hash = claim_vault_rewards.tx_hash
        AND transactions.chain_id = claim_vault_rewards.chain_id
        GROUP BY
            claim_vault_rewards.destination_vault_address
    """
    highest_block_already_fetched_df = _exec_sql_and_cache(query)
    if highest_block_already_fetched_df.empty:
        highest_block_already_fetched = dict()
    else:
        highest_block_already_fetched = highest_block_already_fetched_df.set_index("destination_vault_address")[
            "max_block"
        ].to_dict()

    return highest_block_already_fetched


#
def get_all_destinaitons_vault_addresses() -> list:
    query = """
        SELECT DISTINCT destination_vault_address, chain_id
        FROM autopool_destinations
    """
    all_destination_vault_addresses_df = _exec_sql_and_cache(query)
    return all_destination_vault_addresses_df.to_dict(orient="records")


def fetch_and_flatten_all_claim_vault_rewards_events(
    start_block: int, chain: ChainData, destination_vault_address: str
) -> pd.DataFrame:
    contract = chain.client.eth.contract(
        destination_vault_address, abi=MINIMAL_DESTINATION_VAULT_REWARD_CLAIMED_EVENT_ABI
    )

    all_claim_vault_rewards_events = fetch_events(
        contract.events.RewardsClaimed,
        chain=chain,
        start_block=start_block,
    )
    flat_rewards_claimed_records = flatten_reward_claimed_events(all_claim_vault_rewards_events)
    flat_df = pd.DataFrame(flat_rewards_claimed_records)
    flat_df["destination_vault_address"] = destination_vault_address
    flat_df["chain_id"] = chain.chain_id
    print(f"Total rows: {len(flat_df)}, NaN values: {flat_df.isna().sum().sum()}")

    if flat_df.isna().sum().sum() > 0:
        pass

    flat_df = flat_df.dropna()
    return flat_df


def flatten_reward_claimed_events(all_claim_vault_rewards_events: pd.DataFrame) -> list[dict]:
    flat_rewards_claimed_records = []

    for max_tokens_claimed in range(128):  # in practice it shouldn't be more than 8
        if f"amountsClaimed_{max_tokens_claimed}" not in all_claim_vault_rewards_events.columns:
            break

        amount_claimed_col = f"amountsClaimed_{max_tokens_claimed}"
        token_address_col = f"tokensClaimed_{max_tokens_claimed}"
        sub_df = all_claim_vault_rewards_events[
            ["event", "block", "transaction_index", "log_index", "hash", amount_claimed_col, token_address_col]
        ].copy()

        sub_df.rename(columns={amount_claimed_col: "amount_claimed", token_address_col: "token_address"}, inplace=True)

        rewards_claimed_single_token_records = sub_df.to_dict(orient="records")

        rewards_claimed_single_token_records = [
            r
            for r in rewards_claimed_single_token_records
            if r["token_address"] != ZERO_ADDRESS and pd.notna(r["amount_claimed"])
        ]

        flat_rewards_claimed_records.extend(rewards_claimed_single_token_records)

        if max_tokens_claimed > 8:
            raise ValueError("never think I should get more than 8 tokens claimed in a single tx")

    return flat_rewards_claimed_records


def _fetch_new_claim_vault_rewards_events(
    destination_vault_address: str, start_block: int, chain: ChainData, token_to_decimals
) -> pd.DataFrame:

    flat_df = fetch_and_flatten_all_claim_vault_rewards_events(
        start_block=start_block,
        chain=chain,
        destination_vault_address=destination_vault_address,
    )

    if not flat_df.empty:

        if "0xd2d311c09a3a1cdd7162babec8b26e4bd9ced7be0f272b00d7516e58efb32738" in flat_df["hash"].values:
            print("debug")  # seems fine, not sure what's wrong here
            # claiming 0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B cvx
            # and 0xD533a949740bb3306d119CC777fa900bA034cd52

        flat_df["amount_claimed_normalized"] = flat_df.apply(
            lambda r: int(r["amount_claimed"]) / (10 ** token_to_decimals[r["token_address"]]), axis=1
        )

        def to_claim_vault_reward(row: pd.Series) -> ClaimVaultRewards:
            return ClaimVaultRewards(
                tx_hash=str(row["hash"]),
                log_index=int(row["log_index"]),
                chain_id=int(row["chain_id"]),
                token_address=str(row["token_address"]),
                amount_claimed=float(row["amount_claimed"]),
                destination_vault_address=str(row["destination_vault_address"]),
            )

        new_claim_vault_rewards = flat_df.apply(to_claim_vault_reward, axis=1).tolist()
        return new_claim_vault_rewards
    else:
        return []

def ensure_destination_vault_rewards_claimed_table_is_current() -> pd.DataFrame:
    highest_block_already_fetched = _get_highest_block_already_fetched_by_destination_vault_address()
    token_to_decimals, token_to_symbol = get_token_details_dict()
    del token_to_symbol
    chain_id_to_chain = {chain.chain_id: chain for chain in ALL_CHAINS}
    destination_vault_addresses = get_all_destinaitons_vault_addresses()

    for target_chain in ALL_CHAINS:
        new_claim_vault_rewards_rows = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_destination = {
                executor.submit(
                    _fetch_new_claim_vault_rewards_events,
                    row["destination_vault_address"],
                    highest_block_already_fetched.get(
                        row["destination_vault_address"], target_chain.block_autopool_first_deployed
                    )
                    + 1,
                    target_chain,
                    token_to_decimals,
                ): row
                for row in destination_vault_addresses
                if chain_id_to_chain[row["chain_id"]] == target_chain
            }

            for future in as_completed(future_to_destination):
                row = future_to_destination[future]
                try:
                    new_claim_vault_rewards = future.result()
                    if new_claim_vault_rewards:
                        new_claim_vault_rewards_rows.extend(new_claim_vault_rewards)
                except Exception as exc:
                    print(
                        f"Generated an exception: {exc} for destination vault address: {row['destination_vault_address']} on chain {target_chain.name}"
                    )
                    raise exc

        if new_claim_vault_rewards_rows:
            ensure_all_transactions_are_saved_in_db(
                tx_hashes=[r.tx_hash for r in new_claim_vault_rewards_rows],
                chain=target_chain,
            )
            insert_avoid_conflicts(
                new_claim_vault_rewards_rows,
                ClaimVaultRewards,
            )


if __name__ == "__main__":
    # ensure_destination_vault_rewards_claimed_table_is_current()

    profile_function(ensure_destination_vault_rewards_claimed_table_is_current)
