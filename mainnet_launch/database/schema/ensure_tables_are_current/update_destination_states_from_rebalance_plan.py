import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import pandas as pd
from web3 import Web3

import plotly.express as px

from mainnet_launch.database.schema.full import (
    RebalancePlans,
    Destinations,
    DexSwapSteps,
    Tokens,
    DestinationStates,
    DestinationTokenValues,
)

from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)

from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, USDC, WETH, AUTO_USD
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
    destination_token_states = []
    state_of_destinations: list[DestinationStates] = []
    for dest_state in plan["sod"]["destStates"]:
        # direct lookups and float conversion
        incentive = float(dest_state["incentiveAPR"])
        total_in = float(dest_state["totalAprIn"])
        total_out = float(dest_state["totalAprOut"])

        raw_total_supply = float(dest_state["totSupply"])
        normalized_total_supply = raw_total_supply / (10 ** tokens_address_to_decimals[dest_state["underlying"]])

        current_timestamps = [plan["sod"]["currentTimestamp"] + i for i in range(-30, 30)]
        # the first block that is +- 1 minute from this timestamp, (not is approx)
        for t in current_timestamps:
            block = timestamp_to_block.get(t, None)
            if block is not None:
                break

        state = DestinationStates(
            destination_vault_address=Web3.toChecksumAddress(dest_state["address"]),
            block=block,
            chain_id=autopool.chain.chain_id,
            incentive_apr=incentive,
            fee_apr=None,
            base_apr=None,
            points_apr=None,
            fee_plus_base_apr=total_out - (incentive / 0.9),  # remove downscaling
            total_apr_in=total_in,
            total_apr_out=total_out,
            underlying_token_total_supply=normalized_total_supply,
            safe_total_supply=None,
            lp_token_spot_price=float(dest_state["spotPrice"]),
            lp_token_safe_price=float(dest_state["safePrice"]),
        )

        state_of_destinations.append(state)

        # for index in range(len(dest_state["underlyingTokens"])):
        #     token_address = Web3.toChecksumAddress(dest_state["underlyingTokens"][index])
        #     quantity = dest_state["underlyingTokenAmounts"][index] / (10 ** tokens_address_to_decimals[token_address])
        #     destination_token_states.append(
        #         DestinationTokenValues(
        #             destination_vault_address=Web3.toChecksumAddress(dest_state["address"]),
        #             block=block,
        #             chain_id=autopool.chain.chain_id,
        #             token_address=token_address,
        #             spot_price=dest_state["tokenSpotPrice"][index],
        #             quantity=quantity,
        #         )
        #     )

    return state_of_destinations


def update_destination_states_from_rebalance_plan():
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    for autopool in [AUTO_USD]:

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
            return dicts_to_destination_states(plan, autopool, timestamp_to_block, tokens_address_to_decimals)

        all_destination_states = []
        with ThreadPoolExecutor(max_workers=128) as executor:
            for p in solver_plan_paths_on_remote:
                state_of_destinations = _process_plan(p)
                futures = {executor.submit(_process_plan, path): path for path in solver_plan_paths_on_remote}

                for fut in as_completed(futures):
                    state_of_destinations, destination_token_states = fut.result()
                    all_destination_states.extend(state_of_destinations)
                    # all_dest_token_states.extend(destination_token_states)

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
