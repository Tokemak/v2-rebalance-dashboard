import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from web3 import Web3

from multicall.call import Call

from mainnet_launch.database.schema.full import (
    Tokens,
    DestinationStates,
)

from mainnet_launch.data_fetching.get_state_by_block import get_state_by_one_block
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
)

from mainnet_launch.constants import (
    AutopoolConstants,
    ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN,
)
from mainnet_launch.data_fetching.block_timestamp import _fetch_block_df_from_subgraph, ensure_all_blocks_are_in_table


def convert_rebalance_plan_json_to_rebalance_plan_line(
    rebalance_plan_json_key: str, s3_client, autopool: AutopoolConstants
):
    plan = json.loads(
        s3_client.get_object(
            Bucket=autopool.solver_rebalance_plans_bucket,
            Key=rebalance_plan_json_key,
        )["Body"].read()
    )

    plan["rebalance_plan_json_key"] = rebalance_plan_json_key
    plan["autopool_vault_address"] = autopool.autopool_eth_addr
    return plan


def dicts_to_destination_states(
    plan: dict,
    autopool: AutopoolConstants,
    timestamp_to_block: dict[int, int],
    tokens_address_to_decimals: dict[str, int],
) -> list[DestinationStates]:
    """
    Convert each dict in `items` into a DestinationStates ORM object,
    using direct lookups and computing fee_plus_base_apr as
    total_apr_out - incentive_apr. All numeric fields are cast to float.
    """

    plan_timestamp = plan["sod"]["currentTimestamp"]
    # the first block that is + 1 minute from this timestamp, (not is approx)
    # not totally certain on this logic

    timestamp_block_tuples = [(timestamp, block) for timestamp, block in timestamp_to_block.items()]
    timestamp_block_tuples.sort(lambda x: x[0], reverse=True)

    for timestamp, some_block in timestamp_to_block.items():
        if timestamp >= plan_timestamp:
            block_after_plan_timestamp = some_block

    def _extract_idle_usdc(success, AssetBreakdown):
        if success:
            totalIdle, totalDebt, totalDebtMin, totalDebtMax = AssetBreakdown
            return int(totalIdle) / 1e6

    amount_of_idle_usdc_call = (
        Call(
            autopool.autopool_eth_addr,
            ["getAssetBreakdown()((uint256,uint256,uint256,uint256))"],
            [("idle", _extract_idle_usdc)],
        ),
    )

    quantity_of_idle = get_state_by_one_block([amount_of_idle_usdc_call], block_after_plan_timestamp, autopool.chain)[
        "idle"
    ]

    state_of_destinations: list[DestinationStates] = [
        DestinationStates(
            destination_vault_address=autopool.autopool_eth_addr,
            block=block_after_plan_timestamp,
            chain_id=autopool.chain.chain_id,
            incentive_apr=None,  # not sure if 0 or None makes more sense here
            fee_apr=None,
            base_apr=None,
            points_apr=None,
            fee_plus_base_apr=None,
            total_apr_in=None,
            total_apr_out=None,
            underlying_token_total_supply=quantity_of_idle,
            safe_total_supply=quantity_of_idle,
            lp_token_spot_price=1.0,
            lp_token_safe_price=1.0,
            from_rebalance_plan=True,
            rebalance_plan_timestamp=plan_timestamp,
        )
    ]

    for dest_state in plan["sod"]["destStates"]:
        # direct lookups and float conversion
        incentive = float(dest_state["incentiveAPR"])
        total_in = float(dest_state["totalAprIn"])
        total_out = float(dest_state["totalAprOut"])

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
            fee_plus_base_apr=total_out - (incentive / 0.9),  # remove downscaling
            total_apr_in=total_in,
            total_apr_out=total_out,
            underlying_token_total_supply=underlying_token_total_supply,
            safe_total_supply=None,
            lp_token_spot_price=float(dest_state["spotPrice"]),
            lp_token_safe_price=float(dest_state["safePrice"]),
            from_rebalance_plan=True,
        )
        state_of_destinations.append(state)

    return state_of_destinations


def update_destination_states_from_rebalance_plan():
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    for autopool in ALL_AUTOPOOLS_DATA_FROM_REBALANCE_PLAN:

        solver_plan_paths_on_remote = [
            r["Key"] for r in s3_client.list_objects_v2(Bucket=autopool.solver_rebalance_plans_bucket).get("Contents")
        ]
        plan_timestamps = [int(p.split(("_"))[2]) for p in solver_plan_paths_on_remote]

        block_df = _fetch_block_df_from_subgraph(autopool.chain, plan_timestamps)
        timestamp_to_block = {int(t): b for t, b in zip(block_df["timestamp"], block_df["block"])}
        tokens_orm = get_full_table_as_orm(Tokens, where_clause=(Tokens.chain_id == autopool.chain.chain_id))
        tokens_address_to_decimals = {t.token_address: t.decimals for t in tokens_orm}

        def _process_plan(plan_path):
            plan = convert_rebalance_plan_json_to_rebalance_plan_line(plan_path, s3_client, autopool)
            new_destination_states = dicts_to_destination_states(
                plan, autopool, timestamp_to_block, tokens_address_to_decimals
            )
            return new_destination_states

        all_destination_states = []
        with ThreadPoolExecutor(max_workers=32) as executor:

            futures = {executor.submit(_process_plan, path): path for path in solver_plan_paths_on_remote}

            for fut in as_completed(futures):
                state_of_destinations = fut.result()
                all_destination_states.extend(state_of_destinations)

        ensure_all_blocks_are_in_table([d.block for d in all_destination_states], autopool.chain)
        insert_avoid_conflicts(
            all_destination_states,
            DestinationStates,
            index_elements=[
                DestinationStates.block,
                DestinationStates.chain_id,
                DestinationStates.destination_vault_address,
            ],
        )


import cProfile, pstats

if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    update_destination_states_from_rebalance_plan()
    profiler.disable()
    profiler.dump_stats(
        "mainnet_launch/database/schema/ensure_tables_are_current/update_destination_states_from_rebalance_plan.prof"
    )
    stats = pstats.Stats(
        "mainnet_launch/database/schema/ensure_tables_are_current/update_destination_states_from_rebalance_plan.prof"
    )
    stats.strip_dirs().sort_stats("cumtime").print_stats(30)
