from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pandas as pd
from web3 import Web3


from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, USDC, WETH, DOLA, EURC, USDT
from mainnet_launch.database.schema.full import RebalancePlans, Destinations, DexSwapSteps, Tokens

from mainnet_launch.database.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)


from mainnet_launch.data_fetching.internal.s3_helper import (
    fetch_all_solver_rebalance_plan_file_names,
    make_s3_client,
    fetch_rebalance_plan_json_from_s3_bucket,
)


def _handle_only_state_of_destinations_rebalance_plan(plan: dict) -> RebalancePlans:
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
    possible_amount_out_keys = ["amountOutUSD", "amountOutETH", "amountOutQuote"]
    possible_amount_in_keys = ["minAmountInUSD", "minAmountInETH", "minAmountInQuote"]

    for out_key, in_key in zip(possible_amount_out_keys, possible_amount_in_keys):
        if (out_key in plan) and (in_key in plan):
            amount_out_safe_value = int(plan[out_key]) / 10**autopool.base_asset_decimals
            min_amount_in_safe_value = int(plan[in_key]) / 10**autopool.base_asset_decimals
            return amount_out_safe_value, min_amount_in_safe_value

    raise ValueError(f"unknown safe amount value keys {plan.keys()=}")


def _extract_spot_values(rebalance_test: dict, autopool: AutopoolConstants):
    if autopool.base_asset in USDC:
        spot_value_out_key = "outSpotUSD"
        min_amount_in_spot_value_key = "inSpotUSD"
    elif autopool.base_asset in WETH:
        spot_value_out_key = "outSpotETH"
        min_amount_in_spot_value_key = "inSpotETH"
    elif (autopool.base_asset in DOLA) or (autopool.base_asset in EURC) or (autopool.base_asset in USDT):
        spot_value_out_key = "outSpotQuote"
        min_amount_in_spot_value_key = "inSpotQuote"
    else:
        raise ValueError(f"unexpected base_asset {autopool.base_asset}")

    out_val = rebalance_test.get(spot_value_out_key)
    in_val = rebalance_test.get(min_amount_in_spot_value_key)

    amount_out_spot_value = int(out_val) / 1e18 if out_val is not None else None
    min_amount_in_spot_value = int(in_val) / 1e18 if in_val is not None else None

    return amount_out_spot_value, min_amount_in_spot_value


def _extract_rebalance_plan(
    plan: dict,
    autopool: AutopoolConstants,
    destination_address_to_symbol: dict[str, str],
    token_address_to_decimals: dict[str, int],
) -> RebalancePlans:
    # TODO this is inelegant, consider refactoring
    if plan["sodOnly"] == True:
        new_sod_only_plan = _handle_only_state_of_destinations_rebalance_plan(plan)
        return new_sod_only_plan

    underlying_out_symbol = destination_address_to_symbol[Web3.toChecksumAddress(plan["destinationOut"])]
    underlying_in_symbol = destination_address_to_symbol[Web3.toChecksumAddress(plan["destinationIn"])]

    amount_out, min_amount_in = _extract_normalized_amounts(plan, token_address_to_decimals)
    amount_out_safe_value, min_amount_in_safe_value = _extract_safe_values(plan, autopool)

    projected_swap_cost = amount_out_safe_value - min_amount_in_safe_value

    amount_out_spot_value, min_amount_in_spot_value = _extract_spot_values(plan, autopool)

    rebalance_test = plan.get("rebalanceTest") or {
        "currentTimestamp": None,
        "type": None,
        "outDest": None,
        "outSpotUSD": None,
        "outDestApr": None,
        "inDest": None,
        "inSpotUSD": None,
        "inDestApr": None,
        "inDestAdjApr": None,
        "swapOffsetPeriod": None,
    }

    in_destination_name = rebalance_test["inDest"]
    projected_gross_gain = 0
    candidate_destinations_rank = None
    projected_net_gain = 0

    if plan["destinationIn"] != autopool.autopool_eth_addr:
        for i, d in enumerate(plan.get("addRank", [])):
            if d[0] == in_destination_name:
                candidate_destinations_rank = i
                projected_net_gain = d[1] / 1e18
                projected_gross_gain = projected_net_gain + projected_swap_cost

    out_dest_apr = float(rebalance_test["outDestApr"]) if rebalance_test["outDestApr"] else None
    in_dest_apr = float(rebalance_test["inDestApr"]) if rebalance_test["inDestApr"] else None
    in_dest_adj_apr = float(rebalance_test["inDestAdjApr"]) if rebalance_test["inDestAdjApr"] else None
    apr_delta = (in_dest_adj_apr if in_dest_adj_apr else 0) - (out_dest_apr if out_dest_apr else 0)
    swap_offset_period = int(rebalance_test["swapOffsetPeriod"]) if rebalance_test["swapOffsetPeriod"] else None
    num_candidate_destinations = len(plan.get("addRank", []))
    projected_slippage = 100 * projected_swap_cost / amount_out_safe_value if amount_out_safe_value else None

    new_rebalance_plan_row = RebalancePlans(
        file_name=plan["rebalance_plan_json_key"],
        datetime_generated=pd.to_datetime(int(plan["timestamp"]), unit="s", utc=True),
        autopool_vault_address=plan["autopool_vault_address"],
        chain_id=int(plan["chainId"]),
        solver_address=Web3.toChecksumAddress(plan["solverAddress"]),
        rebalance_type=rebalance_test["type"],
        destination_in=Web3.toChecksumAddress(plan["destinationIn"]),
        token_in=Web3.toChecksumAddress(plan["tokenIn"]),
        destination_out=Web3.toChecksumAddress(plan["destinationOut"]),
        token_out=Web3.toChecksumAddress(plan["tokenOut"]),
        move_name=f"{underlying_out_symbol} -> {underlying_in_symbol}",  # what is a better move name?
        amount_out=amount_out,
        amount_out_safe_value=amount_out_safe_value,
        min_amount_in=min_amount_in,
        min_amount_in_safe_value=min_amount_in_safe_value,
        amount_out_spot_value=amount_out_spot_value,
        out_dest_apr=out_dest_apr,
        min_amount_in_spot_value=min_amount_in_spot_value,
        in_dest_apr=in_dest_apr,
        in_dest_adj_apr=in_dest_adj_apr,
        apr_delta=apr_delta,
        swap_offset_period=swap_offset_period,
        num_candidate_destinations=num_candidate_destinations,
        candidate_destinations_rank=candidate_destinations_rank,
        projected_swap_cost=projected_swap_cost,
        projected_net_gain=projected_net_gain,
        projected_gross_gain=projected_gross_gain,
        projected_slippage=projected_slippage,
    )

    return new_rebalance_plan_row


def _extract_new_dext_steps(plan: dict) -> list[DexSwapSteps]:
    new_dex_steps = []

    def find_agg_names(step_dictionary_details: dict, target="aggregatorName"):
        results = []
        for key, value in step_dictionary_details.items():
            if key == target:
                results.append(value)
            elif isinstance(value, dict):
                results.extend(find_agg_names(value, target))

        return results

    for step_index, step in enumerate(plan["steps"]):
        if step["stepType"] == "swap":

            if step["dex"] == "tokemakApi":
                aggregator_names = "{" + ",".join(find_agg_names(step["payload"], target="aggregatorName")) + "}"
            else:
                aggregator_names = None

            new_dex_steps.append(
                DexSwapSteps(
                    file_name=plan["rebalance_plan_json_key"],
                    step_index=step_index,
                    dex=step["dex"],
                    aggregator_names=aggregator_names,
                )
            )
    return new_dex_steps


def ensure_rebalance_plans_table_are_current():
    destinations: list[Destinations] = get_full_table_as_orm(Destinations)
    destination_address_to_symbol = {d.destination_vault_address: d.underlying_symbol for d in destinations}

    tokens: list[Tokens] = get_full_table_as_orm(Tokens)
    token_address_to_decimals = {t.token_address: t.decimals for t in tokens}

    s3_client = make_s3_client()
    for autopool in ALL_AUTOPOOLS:
        solver_plan_paths_on_remote = fetch_all_solver_rebalance_plan_file_names(autopool, s3_client)

        plans_not_already_fetched = get_subset_not_already_in_column(
            RebalancePlans,
            RebalancePlans.file_name,
            solver_plan_paths_on_remote,
            where_clause=RebalancePlans.autopool_vault_address == autopool.autopool_eth_addr,
        )

        if not plans_not_already_fetched:
            continue

        def _process_plan(plan_on_remote: str):
            # only external call here
            plan = fetch_rebalance_plan_json_from_s3_bucket(plan_on_remote, s3_client, autopool)
            new_rebalance_plan_row = _extract_rebalance_plan(
                plan, autopool, destination_address_to_symbol, token_address_to_decimals
            )
            new_dex_steps_rows = _extract_new_dext_steps(plan)

            return (new_rebalance_plan_row, new_dex_steps_rows)

        all_rebalance_plan_rows = []
        all_dex_steps_rows = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            for response in executor.map(_process_plan, plans_not_already_fetched):
                new_rebalance_plan_row, new_dex_steps_rows = response
                all_rebalance_plan_rows.append(new_rebalance_plan_row)
                all_dex_steps_rows.extend(new_dex_steps_rows)

                # TODO add RebalanceCandidateDestinations here

        insert_avoid_conflicts(all_rebalance_plan_rows, RebalancePlans, index_elements=[RebalancePlans.file_name])
        insert_avoid_conflicts(
            all_dex_steps_rows, DexSwapSteps, index_elements=[DexSwapSteps.file_name, DexSwapSteps.step_index]
        )


def print_count_of_rebalance_plans_in_db():
    s3_client = make_s3_client()

    for autopool in ALL_AUTOPOOLS:
        solver_plan_paths_on_remote = fetch_all_solver_rebalance_plan_file_names(autopool, s3_client)
        print(autopool.name, len(solver_plan_paths_on_remote))


if __name__ == "__main__":

    from mainnet_launch.constants import profile_function

    profile_function(ensure_rebalance_plans_table_are_current)

    print_count_of_rebalance_plans_in_db()
