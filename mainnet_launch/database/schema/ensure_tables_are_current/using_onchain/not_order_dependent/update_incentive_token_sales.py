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


def ensure_incentive_token_swapped_events_are_saved_in_db() -> pd.DataFrame:
    highest_block_already_fetched = _get_highest_swapped_event_already_fetched()
    all_new_inentive_token_swapped_events = []
    chain_to_highest_block = {chain: chain.client.eth.block_number - 500 for chain in ALL_CHAINS}

    for chain in ALL_CHAINS:
        all_swapped_events = []
        token_addresses_to_ensure_we_have_in_db = set()
        for liquidation_row in [LIQUIDATION_ROW, LIQUIDATION_ROW2]:
            start_block = highest_block_already_fetched[(chain.chain_id, liquidation_row(chain))] + 1

            contract = chain.client.eth.contract(liquidation_row(chain), abi=DESTINATION_DEBT_REPORTING_SWAPPED_ABI)
            swapped_df = fetch_events(
                contract.events.Swapped, chain=chain, start_block=start_block, end_block=chain_to_highest_block[chain]
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
    profile_function(ensure_incentive_token_swapped_events_are_saved_in_db)


# first run from 0
# Total time: 51.700065 s

# Timer unit: 1 s

# Total time: 51.7001 s
# File: /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/not_order_dependent/update_incentive_token_sales.py
# Function: ensure_incentive_token_swapped_events_are_saved_in_db at line 109

# Line #      Hits         Time  Per Hit   % Time  Line Contents
# ==============================================================
#    109                                           def ensure_incentive_token_swapped_events_are_saved_in_db() -> pd.DataFrame:
#    110         1          1.5      1.5      2.9      highest_block_already_fetched = _get_highest_swapped_event_already_fetched()
#    111         1          0.0      0.0      0.0      all_new_inentive_token_swapped_events = []
#    112
#    113         4          0.0      0.0      0.0      for chain in ALL_CHAINS:
#    114         3          0.0      0.0      0.0          all_swapped_events = []
#    115         3          0.0      0.0      0.0          token_addresses_to_ensure_we_have_in_db = set()
#    116         9          0.0      0.0      0.0          for liquidation_row in [LIQUIDATION_ROW, LIQUIDATION_ROW2]:
#    117         6          0.0      0.0      0.0              start_block = highest_block_already_fetched[(chain.chain_id, liquidation_row(chain))] + 1
#    118
#    119         6          0.0      0.0      0.1              contract = chain.client.eth.contract(liquidation_row(chain), abi=DESTINATION_DEBT_REPORTING_SWAPPED_ABI)
#    120         6         11.3      1.9     21.9              swapped_df = fetch_events(contract.events.Swapped, chain=chain, start_block=start_block)
#    121         6          0.0      0.0      0.0              swapped_df["liquidation_row"] = liquidation_row(chain)
#    122         6          0.0      0.0      0.0              swapped_df["chain_id"] = chain.chain_id
#    123
#    124         6          0.0      0.0      0.0              if not swapped_df.empty:
#    125         5          0.0      0.0      0.0                  all_swapped_events.append(swapped_df)
#    126
#    127         6          0.0      0.0      0.0              token_addresses_to_ensure_we_have_in_db.update(set(swapped_df["sellTokenAddress"].unique()))
#    128         6          0.0      0.0      0.0              token_addresses_to_ensure_we_have_in_db.update(set(swapped_df["buyTokenAddress"].unique()))
#    129
#    130         3          0.9      0.3      1.7          ensure_all_tokens_are_saved_in_db(list(token_addresses_to_ensure_we_have_in_db), chain)
#    131         3          1.1      0.4      2.1          token_to_decimals, token_to_symbol = get_token_details_dict()
#    132
#    133         3          0.0      0.0      0.0          if all_swapped_events:
#    134         3          0.0      0.0      0.0              all_swapped_events = pd.concat(all_swapped_events)
#    135         3          0.9      0.3      1.7              all_swapped_events = _add_token_details(all_swapped_events, token_to_decimals, token_to_symbol)
#    136                                                   else:
#    137                                                       all_swapped_events = pd.DataFrame()
#    138
#    139         9          0.5      0.1      1.0          new_incentive_token_swapped_events = all_swapped_events.apply(
#    140         3          0.0      0.0      0.0              lambda r: IncentiveTokenSwapped(
#    141                                                           tx_hash=r["hash"],
#    142                                                           log_index=int(r["log_index"]),
#    143                                                           chain_id=int(r["chain_id"]),
#    144                                                           sell_token_address=r["sellTokenAddress"],
#    145                                                           buy_token_address=r["buyTokenAddress"],
#    146                                                           sell_amount=float(r["sellAmount_normalized"]),
#    147                                                           buy_amount=float(r["buyAmount_normalized"]),
#    148                                                           buy_amount_received=float(r["buyTokenAmountReceived_normalized"]),
#    149                                                           liquidation_row=r["liquidation_row"],
#    150                                                       ),
#    151         3          0.0      0.0      0.0              axis=1,
#    152         3          0.0      0.0      0.0          ).tolist()
#    153
#    154         3          0.0      0.0      0.0          all_new_inentive_token_swapped_events.extend(new_incentive_token_swapped_events)
#    155
#    156         3         32.6     10.9     63.1          ensure_all_transactions_are_saved_in_db(list(all_swapped_events["hash"].unique()), chain)
#    157         3          2.9      1.0      5.5          insert_avoid_conflicts(new_incentive_token_swapped_events, IncentiveTokenSwapped)


# some options,

# - ~~Incentive Harvester:  [`0x453BF45e5A9A476C6d6c74D1c8e529C9C27f51e7`](https://etherscan.io/address/0x453BF45e5A9A476C6d6c74D1c8e529C9C27f51e7)~~
# - Incentive Harvester: [`0x4A566dbb39d5b75DA98e1E1fd98F785896178791`](https://etherscan.io/address/0x4A566dbb39d5b75DA98e1E1fd98F785896178791)
# Liquidator: 0x0b1EAA1CF011C80f075958Cf5B6bD49Abc3D7a72

# maybe get the liquidator and


# 0xF570EA70106B8e109222297f9a90dA477658d481 ( most recent liqudation row)


# current
# Liquidation Row: 0xF570EA70106B8e109222297f9a90dA477658d481

# old
# Liquidation Row: 0xBf58810BB1946429830C1f12205331608c470ff5


# emit VaultLiquidated(address(vaultAddress), fromToken, params.buyTokenAddress, amount);
# emit GasUsedForVault(address(vaultAddress), gasUsedPerVault, bytes32("liquidation"));

# emit VaultLiquidated(address(vaultAddress), fromToken, params.buyTokenAddress, amount);
# emit GasUsedForVault(address(vaultAddress), gasUsedPerVault, bytes32("liquidation"));

# event VaultLiquidated(address indexed vault, address indexed fromToken, address indexed toToken, uint256 amount);
# event GasUsedForVault(address indexed vault, uint256 gasAmount, bytes32 action);
#

# vault, looks like a destination vault


# I want the


# https://etherscan.io/tx/0x052b4231be3c2b28480b335085cad1c20ba838cccd5fc98e9c1d39e8502c9f11#eventlog

# VaultLiquidated (index_topic_1 address vault, index_topic_2 address fromToken, index_topic_3 address toToken, uint256 amount)View Source

# 133

# Topics
# 0 0x0272b5a6ff5cab190795f808ef35307240b8bc0011849cb8e15c093b40b22dfb
# 1: vault
# 0x0091Fec1B75013D1b83f4Bb82f0BEC4E256758CB # Tokemak-Dola USD Stablecoin-DOLA/sUSDe string
# 2: fromToken
# 0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B # CVX
# 3: toToken
# 0x865377367054516e17014CcdED1e7d814EDC9ce4 # dola
# Data


# amount :
# 583413391926390257

# at a vault level?


# https://etherscan.io/address/0xe2c7011866db4cc754f1b9b60b2f2999b5b54be4#code


# we also want reward added events

# rewardAdded

from dataclasses import dataclass


@dataclass
class VaultLiquidated:
    tx_hash: str  # primary keys
    log_index: int  # primary keys

    destination_vault_address: str
    from_token_address: str
    to_token_address: str

    liquidated_amount: float  # in terms of to_token_address


# swapped event

# event Swapped(
#     address indexed sellTokenAddress,
#     address indexed buyTokenAddress,
#     uint256 sellAmount,
#     uint256 buyAmount,
#     uint256 buyTokenAmountReceived
# );
