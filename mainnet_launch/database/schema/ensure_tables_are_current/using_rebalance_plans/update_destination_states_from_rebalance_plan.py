import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from web3 import Web3
import time
import random
from tqdm.contrib.concurrent import thread_map

from multicall.call import Call

from mainnet_launch.database.schema.full import Tokens, DestinationStates, TokenValues, DestinationTokenValues
from mainnet_launch.data_fetching.get_state_by_block import get_state_by_one_block
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)

from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN, time_decorator
from mainnet_launch.data_fetching.block_timestamp import (
    ensure_all_blocks_are_in_table,
    get_block_by_timestamp_etherscan,
)

from mainnet_launch.data_fetching.internal.s3_helper import (
    fetch_all_solver_rebalance_plan_file_names,
    make_s3_client,
    fetch_rebalance_plan_json_from_s3_bucket,
)


def _get_quantity_of_base_asset_in_idle(
    autopool: AutopoolConstants, tokens_address_to_decimals, block_after_plan_timestamp
) -> float:
    def _extract_idle_usdc(success, AssetBreakdown):
        if success:
            totalIdle, totalDebt, totalDebtMin, totalDebtMax = AssetBreakdown
            return int(totalIdle) / 10 ** tokens_address_to_decimals[autopool.base_asset]

    amount_of_idle_usdc_call = Call(
        autopool.autopool_eth_addr,
        ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
        [("idle", _extract_idle_usdc)],
    )

    quantity_of_idle = get_state_by_one_block([amount_of_idle_usdc_call], block_after_plan_timestamp, autopool.chain)[
        "idle"
    ]
    return quantity_of_idle


def convert_rebalance_plan_to_rows(
    plan: dict,
    autopool: AutopoolConstants,
    tokens_address_to_decimals: dict[str, int],
) -> list[DestinationStates]:
    """Makes external calls to etherscan, and on http nodes"""

    block_after_plan_timestamp = get_block_by_timestamp_etherscan(
        plan["sod"]["currentTimestamp"], chain=autopool.chain, closest="after"
    )
    quantity_of_idle = _get_quantity_of_base_asset_in_idle(
        autopool, tokens_address_to_decimals, block_after_plan_timestamp
    )

    new_destination_states_rows = _extract_destination_states_rows(
        autopool, tokens_address_to_decimals, plan, block_after_plan_timestamp, quantity_of_idle
    )
    new_token_values_rows = _extract_token_values_data(autopool, plan, block_after_plan_timestamp)
    new_destination_token_values = _extract_destination_token_values(
        autopool, plan, block_after_plan_timestamp, quantity_of_idle
    )

    return new_destination_states_rows, new_token_values_rows, new_destination_token_values


def _extract_destination_token_values(
    autopool: AutopoolConstants, plan: dict, block_after_plan_timestamp: int, quantity_of_idle: int
) -> list[TokenValues]:

    new_destination_token_values: list[DestinationTokenValues] = [
        DestinationTokenValues(
            block=block_after_plan_timestamp,
            chain_id=autopool.chain.chain_id,
            token_address=autopool.base_asset,
            denominated_in=autopool.base_asset,
            destination_vault_address=autopool.autopool_eth_addr,
            spot_price=1.0,
            quantity=quantity_of_idle,
        )
    ]

    for dest_state in plan["sod"]["destStates"]:
        for token_address, spot_price, raw_amount, decimals in zip(
            dest_state["underlyingTokens"],
            dest_state["tokenSpotPrice"],
            dest_state["underlyingTokenAmounts"],
            dest_state["decimals"],
        ):
            new_destination_token_values.append(
                DestinationTokenValues(
                    block=block_after_plan_timestamp,
                    chain_id=autopool.chain.chain_id,
                    token_address=token_address,
                    destination_vault_address=Web3.toChecksumAddress(dest_state["address"]),
                    denominated_in=autopool.base_asset,
                    spot_price=spot_price,
                    quantity=int(raw_amount) / 1e18,  # note in the destination states, everything is in 1e18
                )
            )

    return new_destination_token_values


def _extract_token_values_data(
    autopool: AutopoolConstants, plan: dict, block_after_plan_timestamp: int
) -> list[TokenValues]:
    new_token_values_rows: list[TokenValues] = [
        TokenValues(
            block=block_after_plan_timestamp,
            chain_id=autopool.chain.chain_id,
            token_address=autopool.base_asset,
            denominated_in=autopool.base_asset,
            backing=1.0,
            safe_price=1.0,
        )
    ]

    seen_tokens = set([autopool.base_asset])

    for dest_state in plan["sod"]["destStates"]:
        # for the first ~5 days of autoUSD
        if "tokenBacking" not in dest_state:
            dest_state["tokenBacking"] = [None for _ in range(len(dest_state["underlyingTokens"]))]

        for token_address, backing, safe_price in zip(
            dest_state["underlyingTokens"], dest_state["tokenBacking"], dest_state["tokenSafePrice"]
        ):
            if token_address not in seen_tokens:
                new_token_values_rows.append(
                    TokenValues(
                        block=block_after_plan_timestamp,
                        chain_id=autopool.chain.chain_id,
                        token_address=token_address,
                        denominated_in=autopool.base_asset,
                        backing=backing,
                        safe_price=safe_price,
                    )
                )
                seen_tokens.add(token_address)

    return new_token_values_rows


def _extract_destination_states_rows(
    autopool: AutopoolConstants,
    tokens_address_to_decimals: dict[str, int],
    plan: dict,
    block_after_plan_timestamp: int,
    quantity_of_idle: float,
) -> list[DestinationStates]:

    new_destination_states_rows: list[DestinationStates] = [
        DestinationStates(
            destination_vault_address=autopool.autopool_eth_addr,
            block=block_after_plan_timestamp,
            chain_id=autopool.chain.chain_id,
            incentive_apr=None,
            fee_apr=None,
            base_apr=None,
            points_apr=None,
            fee_plus_base_apr=None,
            total_apr_in=None,
            total_apr_out=None,
            underlying_token_total_supply=quantity_of_idle,
            safe_total_supply=None,
            lp_token_spot_price=1.0,
            lp_token_safe_price=1.0,
            from_rebalance_plan=True,
            rebalance_plan_timestamp=int(plan["sod"]["currentTimestamp"]),
            rebalance_plan_key=plan["rebalance_plan_json_key"],
        )
    ]

    for dest_state in plan["sod"]["destStates"]:

        incentive = dest_state["incentiveAPR"]  # this is un adjusted
        total_in = dest_state["totalAprIn"]
        total_out = dest_state["totalAprOut"]  # these are adjusted

        raw_underlying_token_total_supply = float(dest_state["totSupply"])
        underlying_token_total_supply = raw_underlying_token_total_supply / (
            10 ** tokens_address_to_decimals[dest_state["underlying"]]
        )

        state = DestinationStates(
            destination_vault_address=Web3.toChecksumAddress(dest_state["address"]),
            block=block_after_plan_timestamp,
            chain_id=autopool.chain.chain_id,
            incentive_apr=incentive,
            fee_apr=None,
            base_apr=None,
            points_apr=None,
            fee_plus_base_apr=total_out - (incentive * 0.9),  # remove downscaling
            total_apr_in=total_in,
            total_apr_out=total_out,
            underlying_token_total_supply=underlying_token_total_supply,
            safe_total_supply=None,
            lp_token_spot_price=float(dest_state["spotPrice"]),
            lp_token_safe_price=float(dest_state["safePrice"]),
            from_rebalance_plan=True,
            rebalance_plan_timestamp=int(plan["sod"]["currentTimestamp"]),
            rebalance_plan_key=plan["rebalance_plan_json_key"],
        )
        new_destination_states_rows.append(state)

    return new_destination_states_rows


@time_decorator
def ensure_destination_states_from_rebalance_plan_are_current():
    s3_client = make_s3_client()

    for autopool in ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN:
        solver_plan_paths_on_remote = fetch_all_solver_rebalance_plan_file_names(autopool, s3_client)
        # not certain if actually slower
        plans_to_fetch = get_subset_not_already_in_column(  # much slower than it needs to be
            DestinationStates, DestinationStates.rebalance_plan_key, solver_plan_paths_on_remote, where_clause=None
        )

        if not plans_to_fetch:
            continue

        tokens_orm: list[Tokens] = get_full_table_as_orm(
            Tokens, where_clause=(Tokens.chain_id == autopool.chain.chain_id)
        )
        tokens_address_to_decimals = {t.token_address: t.decimals for t in tokens_orm}

        def _process_plan(plan_path: str):
            i = 0
            while True:
                try:
                    plan = fetch_rebalance_plan_json_from_s3_bucket(plan_path, s3_client, autopool)
                    new_destination_states_rows, new_token_values_rows, new_destination_token_values = (
                        convert_rebalance_plan_to_rows(plan, autopool, tokens_address_to_decimals)
                    )
                    return new_destination_states_rows, new_token_values_rows, new_destination_token_values

                except Exception as e:

                    i += 1
                    sleep_time = random.uniform(1, 5) + i**2
                    print(f"Error processing plan {plan_path}, retrying in {sleep_time:.2}")
                    time.sleep(sleep_time)  # exponential backoff

                    if i == 5:
                        raise e

        all_destination_states = []
        all_new_token_values_rows = []
        all_destination_token_rows = []

        results = thread_map(
            _process_plan,
            plans_to_fetch,
            max_workers=4,
            desc=f"Extracting Destination States from Rebalance Plans for {autopool.name}",
            unit="plan",
        )

        for new_destination_states_rows, new_token_values_rows, new_destination_token_values in results:
            all_destination_states.extend(new_destination_states_rows)
            all_new_token_values_rows.extend(new_token_values_rows)
            all_destination_token_rows.extend(new_destination_token_values)

        all_blocks_to_add = list(set([d.block for d in all_destination_states]))

        ensure_all_blocks_are_in_table(all_blocks_to_add, autopool.chain)
        insert_avoid_conflicts(
            all_destination_states,
            DestinationStates,
            index_elements=[
                DestinationStates.block,
                DestinationStates.chain_id,
                DestinationStates.destination_vault_address,
            ],
        )

        insert_avoid_conflicts(
            all_new_token_values_rows,
            TokenValues,
            index_elements=[
                TokenValues.block,
                TokenValues.chain_id,
                TokenValues.token_address,
            ],
        )

        insert_avoid_conflicts(
            all_destination_token_rows,
            DestinationTokenValues,
            index_elements=[
                DestinationTokenValues.block,
                DestinationTokenValues.chain_id,
                DestinationTokenValues.token_address,
                DestinationTokenValues.destination_vault_address,
            ],
        )


if __name__ == "__main__":
    ensure_destination_states_from_rebalance_plan_are_current()
