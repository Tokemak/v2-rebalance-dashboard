# does not depend on any other tables being current
# this can be run in parallel to other update functions
# cold start: fetch-all; warm start: only new blocks


import pandas as pd
from multicall import Call

from mainnet_launch.constants import (
    ALL_CHAINS,
    ROOT_PRICE_ORACLE,
    INCENTIVE_PRICING_STATS,
    LIQUIDATION_ROW,
    profile_function,
    ChainData,
    ETH_CHAIN,
)
from mainnet_launch.abis import DESTINATION_DEBT_REPORTING_SWAPPED_ABI
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
    get_state_by_one_block,
    identity_with_bool_success,
    safe_normalize_with_bool_success,
)
from mainnet_launch.database.schema.postgres_operations import _exec_sql_and_cache
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
    insert_avoid_conflicts,
)


def ensure_incentive_token_prices_at_liquidation_are_current() -> None:
    """
    Event-sourced, idempotent updater for incentive token achieved vs expected prices
    during liquidation swaps. Matches the Autopool Fees pattern.
    """
    # max_block_by_chain = _get_checkpoint_max_block_by_chain()
    # new_rows: List[IncentiveTokenPriceAtLiquidation] = []

    for chain in ALL_CHAINS:
        start_block = int(max_block_by_chain.get(chain.chain_id, 0)) + 1
        if start_block < 0:
            start_block = 0

        contract = chain.client.eth.contract(LIQUIDATION_ROW(chain), abi=DESTINATION_DEBT_REPORTING_SWAPPED_ABI)
        swapped_df = fetch_events(contract.events.Swapped, chain=chain, start_block=start_block)

        if swapped_df.empty:
            continue

        # Metadata once per batch at a stable block
        token_addresses = sorted(
            set(swapped_df["sellTokenAddress"].unique()).union(swapped_df["buyTokenAddress"].unique())
        )
        ref_block = int(swapped_df["block"].max())
        token_address_to_symbol, token_address_to_decimals = _token_metadata(chain, token_addresses, ref_block)

        # Normalize & achieved price
        swapped_df = _add_achieved_price_column(swapped_df, token_address_to_decimals)

        # Per-block oracle frames (Root Price Oracle & Incentive Pricing Stats)
        oracle_price_df, incentive_price_df = _fetch_oracle_frames(swapped_df, chain)

        # Melt those wide frames so we can join on (block, token_address)
        long_oracle = pd.melt(oracle_price_df, id_vars=["block"], var_name="token_address", value_name="oracle_price")
        long_ips = pd.melt(
            incentive_price_df, id_vars=["block"], var_name="token_address", value_name="incentive_calculator_price"
        )

        # Reward token = sell side of the swap
        base_cols = [
            "hash",  # tx hash from fetch_events
            "log_index",  # unique within tx
            "block",
            "sellTokenAddress",
            "sellAmount",
            "buyTokenAddress",
            "buyTokenAmountReceived",
            "normalized_sell_amount",
            "normalized_buy_amount",
            "achieved_price",
        ]
        base = swapped_df[base_cols].rename(columns={"sellTokenAddress": "token_address"})

        # Attach oracle & incentive prices and a timestamp (from oracle frame index)
        ts_df = oracle_price_df.reset_index()[["timestamp", "block"]]
        base = (
            base.merge(long_oracle, on=["block", "token_address"], how="left")
            .merge(long_ips, on=["block", "token_address"], how="left")
            .merge(ts_df, on="block", how="left")
        )

        base["token_symbol"] = base["token_address"].map(token_address_to_symbol)
        base["chain_id"] = chain.chain_id

        # Ensure transactions exist (independent, safe to call)
        txs = list(base["hash"].drop_duplicates())
        if txs:
            ensure_all_transactions_are_saved_in_db(txs, chain)

        # Build ORM rows (PK: tx_hash + log_index)
        for _, r in base.iterrows():
            new_rows.append(
                IncentiveTokenPriceAtLiquidation(
                    tx_hash=r["hash"],
                    log_index=int(r["log_index"]),
                    chain_id=int(r["chain_id"]),
                    block=int(r["block"]),
                    timestamp=(
                        datetime.fromtimestamp(int(r["timestamp"]), tz=timezone.utc)
                        if pd.notna(r["timestamp"])
                        else datetime.fromtimestamp(0, tz=timezone.utc)
                    ),
                    token_address=str(r["token_address"]),
                    token_symbol=str(r["token_symbol"]),
                    achieved_price=float(r["achieved_price"]),
                    normalized_sell_amount=float(r["normalized_sell_amount"]),
                    normalized_buy_amount=float(r["normalized_buy_amount"]),
                    oracle_price=None if pd.isna(r["oracle_price"]) else float(r["oracle_price"]),
                    incentive_calculator_price=(
                        None if pd.isna(r["incentive_calculator_price"]) else float(r["incentive_calculator_price"])
                    ),
                )
            )

    if not new_rows:
        return

    insert_avoid_conflicts(new_rows, IncentiveTokenPriceAtLiquidation)


if __name__ == "__main__":

    # the ETH liquditaiont row does not have transaction smore recenlty than 140 days ago
    chain = ETH_CHAIN
    contract = chain.client.eth.contract(LIQUIDATION_ROW(chain), abi=DESTINATION_DEBT_REPORTING_SWAPPED_ABI)
    swapped_df = fetch_events(contract.events.Swapped, chain=chain, start_block=chain.block_autopool_first_deployed)

    print(swapped_df.columns)
    print(swapped_df.head())
    print(swapped_df.shape)
    pass

    # profile_function(ensure_incentive_token_prices_at_liquidation_are_current)

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
