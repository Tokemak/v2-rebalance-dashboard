import json
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pandas as pd
from web3 import Web3

from mainnet_launch.database.schema.full import (
    RebalancePlan,
    Destinations,
    DexSwapSteps,
)

from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)

from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants


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


def _handle_only_state_of_destinations_rebalance_plan(plan: dict) -> RebalancePlan:
    # just to make sure the keys are on remote
    return RebalancePlan(
        file_name=plan["rebalance_plan_json_key"],
        datetime_generated=pd.to_datetime(int(plan["timestamp"]), unit="s", utc=True),
        autopool_vault_address=plan["autopool_vault_address"],
        chain_id=int(plan["chainId"]),
        solver_address=None,
        rebalance_type=None,
        destination_in=None,
        token_in=None,
        destination_out=None,
        token_out=None,
        move_name=None,
        amount_out=None,
        amount_out_safe_value=None,
        min_amount_in=None,
        min_amount_in_safe_value=None,
        out_spot_eth=None,
        out_dest_apr=None,
        in_spot_eth=None,
        in_dest_apr=None,
        in_dest_adj_apr=None,
        apr_delta=None,
        swap_offset_period=None,
        num_candidate_destinations=None,
        candidate_destinations_rank=None,
        projected_swap_cost=None,
        projected_net_gain=None,
        projected_gross_gain=None,
        projected_slippage=None,
    )


def _extract_rebalance_plan_and_dex_steps(
    plan: dict, autopool: AutopoolConstants, destinations: list[Destinations]
) -> tuple[RebalancePlan, list[DexSwapSteps]]:
    if plan["sodOnly"] == True:
        new_sod_only_plan = _handle_only_state_of_destinations_rebalance_plan(plan)
        return new_sod_only_plan, []

    dest_map = {Web3.toChecksumAddress(d.destination_vault_address): d.underlying_symbol for d in destinations}

    # then pull out your four values in two lines
    out_addr = Web3.toChecksumAddress(plan["destinationOut"])
    in_addr = Web3.toChecksumAddress(plan["destinationIn"])
    underlying_out_symbol = dest_map[out_addr]
    underlying_in_symbol = dest_map[in_addr]

    in_destination_name = plan["rebalanceTest"]["inDest"]

    projected_swap_cost = (int(plan["amountOutETH"]) / 1e18) - (int(plan["minAmountInETH"]) / 1e18)

    if plan["destinationIn"] == autopool.autopool_eth_addr:
        projected_gross_gain = 0
        candidate_destinations_rank = None
        projected_net_gain = projected_swap_cost
    else:
        for i, d in enumerate(plan["addRank"]):
            if d[0] == in_destination_name:
                candidate_destinations_rank = i
                projected_net_gain = d[1] / 1e18
                projected_gross_gain = projected_net_gain + projected_swap_cost

    new_rebalance_plan_row = RebalancePlan(
        file_name=plan["rebalance_plan_json_key"],
        datetime_generated=pd.to_datetime(int(plan["timestamp"]), unit="s", utc=True),
        autopool_vault_address=plan["autopool_vault_address"],
        chain_id=int(plan["chainId"]),
        solver_address=Web3.toChecksumAddress(plan["solverAddress"]),
        rebalance_type=plan["rebalanceTest"]["type"],
        destination_in=Web3.toChecksumAddress(plan["destinationOut"]),
        token_in=Web3.toChecksumAddress(plan["tokenOut"]),
        destination_out=Web3.toChecksumAddress(plan["destinationIn"]),
        token_out=Web3.toChecksumAddress(plan["tokenIn"]),
        move_name=f"{underlying_out_symbol} -> {underlying_in_symbol}",
        # NOTE: this might amountOutETH might be different for autoUSD, not certain what decimals it is
        amount_out=int(plan["amountOut"]) / 1e18,
        amount_out_safe_value=int(plan["amountOutETH"]) / 1e18,
        min_amount_in=int(plan["minAmountIn"]) / 1e18,
        min_amount_in_safe_value=int(plan["minAmountInETH"]) / 1e18,
        # rebalanceTest values
        out_spot_eth=int(plan["rebalanceTest"]["outSpotETH"]) / 1e18,
        out_dest_apr=float(plan["rebalanceTest"]["outDestApr"]),
        in_spot_eth=int(plan["rebalanceTest"]["inSpotETH"]) / 1e18,
        in_dest_apr=float(plan["rebalanceTest"]["outDestApr"]),
        in_dest_adj_apr=float(plan["rebalanceTest"]["inDestAdjApr"]),
        apr_delta=float(plan["rebalanceTest"]["inDestAdjApr"]) - float(plan["rebalanceTest"]["outDestApr"]),
        swap_offset_period=int(plan["rebalanceTest"]["swapOffsetPeriod"]),
        num_candidate_destinations=len(plan["addRank"]),
        candidate_destinations_rank=candidate_destinations_rank,
        projected_swap_cost=projected_swap_cost,
        projected_net_gain=projected_net_gain,
        projected_gross_gain=projected_gross_gain,
        projected_slippage=100 * projected_swap_cost / int(plan["rebalanceTest"]["outSpotETH"]) / 1e18,  # out spot eth
    )

    new_dex_steps = []

    for step_index, step in enumerate(plan["steps"]):
        if step["stepType"] == "swap":
            new_dex_steps.append(
                DexSwapSteps(file_name=plan["rebalance_plan_json_key"], step_index=step_index, dex=step["dex"])
            )

    return new_rebalance_plan_row, new_dex_steps


def ensure_rebalance_plans_table_are_current():
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    for autopool in ALL_AUTOPOOLS:

        solver_plan_paths_on_remote = [
            r["Key"] for r in s3_client.list_objects_v2(Bucket=autopool.solver_rebalance_plans_bucket).get("Contents")
        ]
        plans_not_already_fetched = get_subset_not_already_in_column(
            RebalancePlan,
            RebalancePlan.file_name,
            solver_plan_paths_on_remote,
            where_clause=RebalancePlan.autopool_vault_address == autopool.autopool_eth_addr,
        )

        destinations = get_full_table_as_orm(
            Destinations, where_clause=Destinations.chain_id == autopool.chain.chain_id
        )

        all_rebalance_plan_rows = []
        all_dex_steps_rows = []

        def _process_plan(plan_on_remote):
            plan = convert_rebalance_plan_json_to_rebalance_plan_line(plan_on_remote, s3_client, autopool)
            return _extract_rebalance_plan_and_dex_steps(plan, autopool, destinations)

        all_rebalance_plan_rows = []
        all_dex_steps_rows = []

        with ThreadPoolExecutor(max_workers=32) as executor:
            for new_rebalance_plan_row, new_dex_steps_rows in executor.map(_process_plan, plans_not_already_fetched):
                all_rebalance_plan_rows.append(new_rebalance_plan_row)
                all_dex_steps_rows.extend(new_dex_steps_rows)

        insert_avoid_conflicts(all_rebalance_plan_rows, RebalancePlan, index_elements=[RebalancePlan.file_name])
        insert_avoid_conflicts(
            all_dex_steps_rows, DexSwapSteps, index_elements=[DexSwapSteps.file_name, DexSwapSteps.step_index]
        )


if __name__ == "__main__":
    ensure_rebalance_plans_table_are_current()
