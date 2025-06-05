import json
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pandas as pd
from web3 import Web3

from mainnet_launch.database.schema.full import RebalancePlans, Destinations, DexSwapSteps, Tokens

from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)

from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, USDC, WETH, DOLA

# todo the scale on the rebalance safe amoutn in and min safe amount out is wrong, way too small for autoUSD
# also safe the plans locally as well, just ot have them


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


def _handle_only_state_of_destinations_rebalance_plan(plan: dict) -> RebalancePlans:
    # just to make sure the keys are on remote
    return RebalancePlans(
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
        amount_out_spot_value=None,
        out_dest_apr=None,
        min_amount_in_spot_value=None,
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


def _extract_normalized_amounts(plan: dict, token_address_to_decimals: dict):

    out_decimals = token_address_to_decimals[plan["tokenOut"]]
    in_decimals = token_address_to_decimals[plan["tokenIn"]]

    amount_out = int(Decimal(plan["amountOut"])) / 10**out_decimals
    min_amount_in = int(Decimal(plan["minAmountIn"])) / 10**in_decimals

    return amount_out, min_amount_in


def _extract_safe_values(plan: dict, autopool: AutopoolConstants):

    if autopool.base_asset in USDC:
        amount_out_key = "amountOutUSD"
        min_amount_in_key = "minAmountInUSD"

    elif autopool.base_asset in WETH:
        amount_out_key = "amountOutETH"
        min_amount_in_key = "minAmountInETH"

    elif autopool.base_asset in DOLA:
        amount_out_key = "amountOutQuote"
        min_amount_in_key = "minAmountInQuote"
    else:
        raise ValueError(f"Unexpected {autopool.base_asset=}")

    amount_out_safe_value = int(plan[amount_out_key]) / 1e18
    min_amount_in_safe_value = int(plan[min_amount_in_key]) / 1e18

    return amount_out_safe_value, min_amount_in_safe_value


def _extract_spot_values(plan: dict, autopool: AutopoolConstants):

    if autopool.base_asset in USDC:
        spot_value_out_key = "outSpotUSD"
        min_amount_in_spot_value_key = "inSpotUSD"

    elif autopool.base_asset in WETH:
        spot_value_out_key = "outSpotETH"
        min_amount_in_spot_value_key = "inSpotETH"
    elif autopool.base_asset in DOLA:
        spot_value_out_key = "outSpotQuote"
        min_amount_in_spot_value_key = "inSpotQuote"

    else:
        raise ValueError(f"Unexpected {autopool.base_asset=}")

    amount_out_spot_value = int(plan["rebalanceTest"][spot_value_out_key]) / 1e18
    min_amount_in_spot_value = (
        int(plan["rebalanceTest"][min_amount_in_spot_value_key]) / 1e18
    )  # not ceratin here about size

    return amount_out_spot_value, min_amount_in_spot_value


def _extract_rebalance_plan_and_dex_steps(
    plan: dict,
    autopool: AutopoolConstants,
    destination_address_to_symbol: dict[str, str],
    token_address_to_decimals: dict[str, int],
) -> tuple[RebalancePlans, list[DexSwapSteps]]:
    if plan["sodOnly"] == True:
        new_sod_only_plan = _handle_only_state_of_destinations_rebalance_plan(plan)
        return new_sod_only_plan, []

    underlying_out_symbol = destination_address_to_symbol[Web3.toChecksumAddress(plan["destinationOut"])]
    underlying_in_symbol = destination_address_to_symbol[Web3.toChecksumAddress(plan["destinationIn"])]

    amount_out, min_amount_in = _extract_normalized_amounts(plan, token_address_to_decimals)
    amount_out_safe_value, min_amount_in_safe_value = _extract_safe_values(plan, autopool)

    projected_swap_cost = amount_out_safe_value - min_amount_in_safe_value

    amount_out_spot_value, min_amount_in_spot_value = _extract_spot_values(plan, autopool)

    in_destination_name = plan["rebalanceTest"]["inDest"]
    projected_gross_gain = 0
    candidate_destinations_rank = None
    projected_net_gain = projected_swap_cost
    if plan["destinationIn"] != autopool.autopool_eth_addr:
        for i, d in enumerate(plan["addRank"]):
            if d[0] == in_destination_name:
                candidate_destinations_rank = i
                projected_net_gain = d[1] / 1e18
                projected_gross_gain = projected_net_gain + projected_swap_cost

    new_rebalance_plan_row = RebalancePlans(
        file_name=plan["rebalance_plan_json_key"],
        datetime_generated=pd.to_datetime(int(plan["timestamp"]), unit="s", utc=True),
        autopool_vault_address=plan["autopool_vault_address"],
        chain_id=int(plan["chainId"]),
        solver_address=Web3.toChecksumAddress(plan["solverAddress"]),
        rebalance_type=plan["rebalanceTest"]["type"],
        destination_in=Web3.toChecksumAddress(plan["destinationIn"]),
        token_in=Web3.toChecksumAddress(plan["tokenIn"]),
        destination_out=Web3.toChecksumAddress(plan["destinationOut"]),
        token_out=Web3.toChecksumAddress(plan["tokenOut"]),
        move_name=f"{underlying_out_symbol} -> {underlying_in_symbol}",
        # NOTE: this might amountOutETH might be different for autoUSD, not certain what decimals it is
        amount_out=amount_out,
        amount_out_safe_value=amount_out_safe_value,
        min_amount_in=min_amount_in,
        min_amount_in_safe_value=min_amount_in_safe_value,
        # rebalanceTest values
        amount_out_spot_value=amount_out_spot_value,
        out_dest_apr=float(plan["rebalanceTest"]["outDestApr"]),
        min_amount_in_spot_value=min_amount_in_spot_value,
        in_dest_apr=float(plan["rebalanceTest"]["outDestApr"]),
        in_dest_adj_apr=float(plan["rebalanceTest"]["inDestAdjApr"]),
        apr_delta=float(plan["rebalanceTest"]["inDestAdjApr"]) - float(plan["rebalanceTest"]["outDestApr"]),
        swap_offset_period=int(plan["rebalanceTest"]["swapOffsetPeriod"]),
        num_candidate_destinations=len(plan["addRank"]),
        candidate_destinations_rank=candidate_destinations_rank,
        projected_swap_cost=projected_swap_cost,
        projected_net_gain=projected_net_gain,
        projected_gross_gain=projected_gross_gain,
        projected_slippage=100 * projected_swap_cost / amount_out_safe_value,  # todo add spot and safe slippage
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
            RebalancePlans,
            RebalancePlans.file_name,
            solver_plan_paths_on_remote,
            where_clause=RebalancePlans.autopool_vault_address == autopool.autopool_eth_addr,
        )

        destinations: list[Destinations] = get_full_table_as_orm(
            Destinations, where_clause=Destinations.chain_id == autopool.chain.chain_id
        )

        destination_address_to_symbol = {d.destination_vault_address: d.underlying_symbol for d in destinations}

        tokens: list[Tokens] = get_full_table_as_orm(Tokens, where_clause=Tokens.chain_id == autopool.chain.chain_id)
        token_address_to_decimals = {t.token_address: t.decimals for t in tokens}

        all_rebalance_plan_rows = []
        all_dex_steps_rows = []

        def _process_plan(plan_on_remote):
            plan = convert_rebalance_plan_json_to_rebalance_plan_line(plan_on_remote, s3_client, autopool)
            return _extract_rebalance_plan_and_dex_steps(
                plan, autopool, destination_address_to_symbol, token_address_to_decimals
            )

        all_rebalance_plan_rows = []
        all_dex_steps_rows = []

        with ThreadPoolExecutor(max_workers=128) as executor:
            for new_rebalance_plan_row, new_dex_steps_rows in executor.map(_process_plan, plans_not_already_fetched):
                all_rebalance_plan_rows.append(new_rebalance_plan_row)
                all_dex_steps_rows.extend(new_dex_steps_rows)
                # TODO add RebalanceCandidateDestinations here

        insert_avoid_conflicts(all_rebalance_plan_rows, RebalancePlans, index_elements=[RebalancePlans.file_name])
        insert_avoid_conflicts(
            all_dex_steps_rows, DexSwapSteps, index_elements=[DexSwapSteps.file_name, DexSwapSteps.step_index]
        )


def print_count_of_rebalance_plans_in_db():
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    for autopool in ALL_AUTOPOOLS:

        solver_plan_paths_on_remote = [
            r["Key"] for r in s3_client.list_objects_v2(Bucket=autopool.solver_rebalance_plans_bucket).get("Contents")
        ]
        print(autopool.autopool_eth_addr, len(solver_plan_paths_on_remote))


if __name__ == "__main__":
    # print_count_of_rebalance_plans_in_db()
    ensure_rebalance_plans_table_are_current()
