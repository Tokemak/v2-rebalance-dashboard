import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.abis import DESTINATION_DEBT_REPORTING_SWAPPED_ABI
from mainnet_launch.data_fetching.alchemy.get_events import fetch_events

from mainnet_launch.database.views import get_token_details_dict
from mainnet_launch.database.schema.full import IncentiveTokenSwapped
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache, insert_avoid_conflicts
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
    highest_block_already_fetched = get_last_processed_block_for_table(IncentiveTokenSwapped)

    for chain in ALL_CHAINS:
        addresses = [addr for addr in [LIQUIDATION_ROW(chain), LIQUIDATION_ROW2(chain)] if addr is not DEAD_ADDRESS]
        top_block = chain.get_block_near_top()
        contract = chain.client.eth.contract(addresses[0], abi=DESTINATION_DEBT_REPORTING_SWAPPED_ABI)
        swapped_df = fetch_events(
            contract.events.Swapped,
            chain=chain,
            start_block=highest_block_already_fetched[chain.chain_id],
            end_block=top_block,
            addresses=addresses,
        )

        swapped_df["chain_id"] = chain.chain_id
        swapped_df["liquidation_row"] = swapped_df["address"]

        tokens_to_make_sure_we_have_in_db = set()
        tokens_to_make_sure_we_have_in_db.update(set(swapped_df["sellTokenAddress"].unique()))
        tokens_to_make_sure_we_have_in_db.update(set(swapped_df["buyTokenAddress"].unique()))
        ensure_all_tokens_are_saved_in_db(list(tokens_to_make_sure_we_have_in_db), chain)
        token_to_decimals, token_to_symbol = get_token_details_dict()

        if swapped_df.empty:
            # early continue if there are no new swapped events
            print(f"No new IncentiveTokenSwapped events for chain {chain.name}")
            write_last_processed_block(chain, top_block, IncentiveTokenSwapped)
            continue

        swapped_df = _add_token_details(swapped_df, token_to_decimals, token_to_symbol)

        new_incentive_token_swapped_events = swapped_df.apply(
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

        ensure_all_transactions_are_saved_in_db(list(swapped_df["hash"].unique()), chain)

        insert_avoid_conflicts(new_incentive_token_swapped_events, IncentiveTokenSwapped)
        print(
            f"Inserted {len(new_incentive_token_swapped_events):,} new IncentiveTokenSwapped events for chain {chain.name}"
        )
        write_last_processed_block(chain, top_block, IncentiveTokenSwapped)


if __name__ == "__main__":

    profile_function(ensure_incentive_token_swapped_events_are_current)
    # ensure_incentive_token_swapped_events_are_current()
