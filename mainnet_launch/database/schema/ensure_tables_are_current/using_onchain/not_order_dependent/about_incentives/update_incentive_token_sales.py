from dataclasses import dataclass
import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.abis import DESTINATION_DEBT_REPORTING_SWAPPED_ABI
from mainnet_launch.data_fetching.get_events import fetch_events

from mainnet_launch.database.schema.views import get_token_details_dict
from mainnet_launch.database.schema.full import IncentiveTokenSwapped
from mainnet_launch.database.schema.postgres_operations import _exec_sql_and_cache, insert_avoid_conflicts
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_destinations_tokens_and_autopoolDestinations_table import (
    ensure_all_tokens_are_saved_in_db,
)


def _get_highest_swapped_event_already_fetched() -> dict:
    query = """
        WITH swapped_events_with_blocks AS (
            SELECT
                its.tx_hash,
                its.liquidation_row,
                its.chain_id,
                t.block
            FROM
                incentive_token_swapped AS its
                JOIN transactions AS t
                    ON t.tx_hash = its.tx_hash
                    AND
                    t.chain_id = its.chain_id
        )
        SELECT
            liquidation_row,
            chain_id,
            MAX(block) AS max_block
        FROM
            swapped_events_with_blocks
        GROUP BY
            chain_id,
            liquidation_row;
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

    # ensure_incentive_token_swapped_events_are_saved_in_db()
    profile_function(ensure_incentive_token_swapped_events_are_current)


# Total time: 289.359 s
# File: /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/ensure_all_tables_are_current.py
# Function: ensure_database_is_current_old at line 131

# Line #      Hits         Time  Per Hit   % Time  Line Contents
# ==============================================================
#    131                                           def ensure_database_is_current_old(echo_sql_to_console: bool = False):
#    132         1          0.0      0.0      0.0      ENGINE.echo = echo_sql_to_console
#    133
#    134         1         66.5     66.5     23.0      ensure_blocks_is_current()
#    135         1          0.9      0.9      0.3      ensure_autopools_are_current()
#    136         1         16.0     16.0      5.5      ensure__destinations__tokens__and__destination_tokens_are_current()
#    137
#    138         1          8.9      8.9      3.1      update_tokemak_EOA_gas_costs_based_on_highest_block_already_fetched()  # independent
#    139         1          5.3      5.3      1.8      ensure_chainlink_gas_costs_table_is_updated()  # idependent
#    140         1          9.8      9.8      3.4      ensure_autopool_fees_are_current()  # independent
#    141
#    142         1          3.6      3.6      1.3      ensure_incentive_token_swapped_events_are_current()  # fully independent
#    143         1          0.3      0.3      0.1      ensure_incentive_token_prices_are_current()  # fully independent
#    144
#    145         1          8.7      8.7      3.0      ensure_destination_underlying_deposits_are_current()  # depends on destinations
#    146         1          8.3      8.3      2.9      ensure_destination_underlying_withdraw_are_current()  #  depends on destinations
#    147
#    148
#    149         1         16.3     16.3      5.6      ensure_destination_states_from_rebalance_plan_are_current()  # big,
#    150         1          2.2      2.2      0.8      ensure_destination_states_are_current()
#    151         1         34.2     34.2     11.8      ensure_destination_token_values_are_current()
#    152         1         20.5     20.5      7.1      ensure_autopool_destination_states_are_current()
#    153         1         11.0     11.0      3.8      ensure_autopool_states_are_current()
#    154         1         27.0     27.0      9.3      ensure_token_values_are_current()
#    155
#    156         1         17.1     17.1      5.9      ensure_rebalance_plans_table_are_current()  # big
#    157         1         32.8     32.8     11.3      ensure_rebalance_events_are_current()
