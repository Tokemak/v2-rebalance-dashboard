from dataclasses import dataclass
import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.abis import DESTINATION_DEBT_REPORTING_SWAPPED_ABI
from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.database.views import get_token_details_dict
from mainnet_launch.database.schema.full import IncentiveTokenSwapped
from mainnet_launch.database.schema.postgres_operations import _exec_sql_and_cache, insert_avoid_conflicts
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_destinations_tokens_and_autopoolDestinations_table import (
    ensure_all_tokens_are_saved_in_db,
)


def _get_highest_swapped_event_already_fetched() -> dict:
    query = """
    SELECT
        incentive_token_swapped.liquidation_row,
        incentive_token_swapped.chain_id,
        MAX(transactions.block) AS max_block
    FROM incentive_token_swapped
    JOIN transactions
        ON transactions.tx_hash = incentive_token_swapped.tx_hash
    AND transactions.chain_id = incentive_token_swapped.chain_id
    GROUP BY
        incentive_token_swapped.chain_id,
        incentive_token_swapped.liquidation_row;
    """
    highest_block_already_fetched = _exec_sql_and_cache(query)
    if highest_block_already_fetched.empty:
        highest_block_already_fetched = dict()
    else:
        highest_block_already_fetched = {
            (row["chain_id"], row["liquidation_row"]): row["max_block"]
            for _, row in highest_block_already_fetched.iterrows()
        }
    for liquidation_row in [LIQUIDATION_ROW, LIQUIDATION_ROW2]:
        for chain in ALL_CHAINS:
            if (chain.chain_id, liquidation_row(chain)) not in highest_block_already_fetched:
                highest_block_already_fetched[(chain.chain_id, liquidation_row(chain))] = (
                    chain.block_autopool_first_deployed
                )

    return highest_block_already_fetched


def _add_token_details(
    all_swapped_events: pd.DataFrame, token_to_decimals: dict, token_to_symbol: dict
) -> pd.DataFrame:
    all_swapped_events["sellTokenAddress"] = all_swapped_events["sellTokenAddress"].apply(
        lambda x: Web3.toChecksumAddress(x)
    )
    all_swapped_events["buyTokenAddress"] = all_swapped_events["buyTokenAddress"].apply(
        lambda x: Web3.toChecksumAddress(x)
    )
    all_swapped_events["sellTokenAddress_decimals"] = all_swapped_events["sellTokenAddress"].map(token_to_decimals)
    all_swapped_events["buyTokenAddress_decimals"] = all_swapped_events["buyTokenAddress"].map(token_to_decimals)
    all_swapped_events["sellTokenAddress_symbol"] = all_swapped_events["sellTokenAddress"].map(token_to_symbol)
    all_swapped_events["buyTokenAddress_symbol"] = all_swapped_events["buyTokenAddress"].map(token_to_symbol)

    all_swapped_events["sellAmount_normalized"] = all_swapped_events["sellAmount"] / (
        10 ** all_swapped_events["sellTokenAddress_decimals"]
    )
    all_swapped_events["buyAmount_normalized"] = all_swapped_events["buyAmount"] / (
        10 ** all_swapped_events["buyTokenAddress_decimals"]
    )
    all_swapped_events["buyTokenAmountReceived_normalized"] = all_swapped_events["buyTokenAmountReceived"] / (
        10 ** all_swapped_events["buyTokenAddress_decimals"]
    )

    return all_swapped_events


def ensure_incentive_token_swapped_events_are_current() -> pd.DataFrame:
    highest_block_already_fetched = _get_highest_swapped_event_already_fetched()
    all_new_inentive_token_swapped_events = []

    for chain in ALL_CHAINS:
        all_swapped_events = []
        token_addresses_to_ensure_we_have_in_db = set()
        for liquidation_row in [LIQUIDATION_ROW, LIQUIDATION_ROW2]:
            start_block = highest_block_already_fetched[(chain.chain_id, liquidation_row(chain))] + 1

            contract = chain.client.eth.contract(liquidation_row(chain), abi=DESTINATION_DEBT_REPORTING_SWAPPED_ABI)
            swapped_df = fetch_events(
                contract.events.Swapped,
                chain=chain,
                start_block=start_block,
            )
            swapped_df["liquidation_row"] = liquidation_row(chain)
            swapped_df["chain_id"] = chain.chain_id

            if not swapped_df.empty:
                all_swapped_events.append(swapped_df)

            token_addresses_to_ensure_we_have_in_db.update(set(swapped_df["sellTokenAddress"].unique()))
            token_addresses_to_ensure_we_have_in_db.update(set(swapped_df["buyTokenAddress"].unique()))

        ensure_all_tokens_are_saved_in_db(list(token_addresses_to_ensure_we_have_in_db), chain)
        token_to_decimals, token_to_symbol = get_token_details_dict()

        if all_swapped_events:
            all_swapped_events = pd.concat(all_swapped_events)
            all_swapped_events = _add_token_details(all_swapped_events, token_to_decimals, token_to_symbol)
        else:
            all_swapped_events = pd.DataFrame()

        if all_swapped_events.empty:
            # early continue if there are no new swapped events
            continue
        else:
            new_incentive_token_swapped_events = all_swapped_events.apply(
                lambda r: IncentiveTokenSwapped(
                    tx_hash=r["hash"],
                    log_index=int(r["log_index"]),
                    chain_id=int(r["chain_id"]),
                    sell_token_address=r["sellTokenAddress"],
                    buy_token_address=r["buyTokenAddress"],
                    sell_amount=float(r["sellAmount_normalized"]),
                    buy_amount=float(r["buyAmount_normalized"]),
                    buy_amount_received=float(r["buyTokenAmountReceived_normalized"]),
                    liquidation_row=r["liquidation_row"],
                ),
                axis=1,
            ).tolist()

            all_new_inentive_token_swapped_events.extend(new_incentive_token_swapped_events)

            ensure_all_transactions_are_saved_in_db(list(all_swapped_events["hash"].unique()), chain)
            insert_avoid_conflicts(new_incentive_token_swapped_events, IncentiveTokenSwapped)


if __name__ == "__main__":

    profile_function(ensure_incentive_token_swapped_events_are_current)
