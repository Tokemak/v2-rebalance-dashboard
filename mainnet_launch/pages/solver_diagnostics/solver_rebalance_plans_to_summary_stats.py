import pandas as pd

from mainnet_launch.pages.solver_diagnostics.solver_diagnostics import (
    _load_solver_df,
    ensure_all_rebalance_plans_are_loaded_from_s3_bucket,
    SOLVER_REBALANCE_PLANS_DIR,
    AUTO_ETH,
    load_solver_plans,
)
import json
import functools


def _get_destination_states(plan: dict):
    if "sod" in plan and "destStates" in plan["sod"]:
        return plan["sod"]["destStates"]
    elif "destStates" in plan:
        return plan["destStates"]
    else:
        raise KeyError("No destStates found in the provided plan JSON.")


def _set_value_or_raise(dict_to_update:dict, key, value) ->None
    if key not in dict_to_update:
        dict_to_update[key] = value
    else:
        # sanity check that the same value is the same in each spot
        if dict_to_update[key] != value:
            raise ValueError("unexpected mismatch", f"{dict_to_update=} {dict_to_update[key]=} {value=}")


def _extract_data(plan: dict):
    """
    extract out the by destination level data for each autopool state

    (lptoken, token) spot price

    (token) safe price

    (lptoken) spot price

    (lptoken) safe price

    (lptoken) autopool owned shares

    (lptoken, token) pool quantity


    # apr data

    incentive, total apr in, total apr out,

    """
    date_info = {"timestamp": plan["timestamp"], "date": pd.to_datetime(plan["timestamp"], unit="s", utc=True)}
    (
        token_safe_prices,
        token_spot_prices,
        pool_spot_prices,
        pool_safe_prices,
        autopool_owned_shares,
        pool_token_quantity,
        destination_incentive_apr,
        destination_total_apr_in,
        destination_total_apr_out,
        destination_fee_and_base_apr,
    ) = [date_info.copy() for _ in range(10)]

    def _extract_price_information_from_solver(dest: dict):

        if "decimals" not in dest:
            decimals = [18 for _ in dest["underlyingTokens"]]
        else:
            decimals = dest["decimals"]
        for i in range(len(decimals)):
            token_address = dest["underlyingTokens"][i]
            underlying_lp_token_address = dest["underlying"]

            _set_value_or_raise(token_safe_prices, token_address, dest["tokenSafePrice"][i])
            _set_value_or_raise(
                token_spot_prices, (underlying_lp_token_address, token_address), dest["tokenSpotPrice"][i]
            )

            _set_value_or_raise(pool_spot_prices, underlying_lp_token_address, dest["spotPrice"])
            _set_value_or_raise(pool_safe_prices, underlying_lp_token_address, dest["safePrice"])

            _set_value_or_raise(autopool_owned_shares, underlying_lp_token_address, dest["ownedShares"] / 1e18)
            _set_value_or_raise(
                pool_token_quantity,
                (underlying_lp_token_address, token_address),
                dest["underlyingTokenAmounts"][i] / 10 ** decimals[i],
            )

            # apr data
            _set_value_or_raise(destination_incentive_apr, underlying_lp_token_address, 100 * dest["incentiveAPR"])
            _set_value_or_raise(destination_total_apr_in, underlying_lp_token_address, 100 * dest["totalAprIn"])
            _set_value_or_raise(destination_total_apr_out, underlying_lp_token_address, 100 * dest["totalAprOut"])

            _set_value_or_raise(
                destination_fee_and_base_apr,
                underlying_lp_token_address,
                100 * (dest["totalAprOut"] - dest["incentiveAPR"]),
            )

    for dest in _get_destination_states(plan):
        _extract_price_information_from_solver(dest)

    return (
        token_safe_prices,
        token_spot_prices,
        pool_spot_prices,
        pool_safe_prices,
        autopool_owned_shares,
        pool_token_quantity,
        destination_incentive_apr,
        destination_total_apr_in,
        destination_total_apr_out,
        destination_fee_and_base_apr,
    )




token_safe_prices_records = []
token_spot_prices_records = []
pool_spot_prices_records = []
pool_safe_prices_records = []
autopool_owned_shares_records = []
pool_token_quantity_records = []
destination_incentive_apr_records = []
destination_total_apr_in_records = []
destination_total_apr_out_records = []
destination_fee_and_base_apr_records = []

for plan in autoETH_plans:
    (
        token_safe_prices,
        token_spot_prices,
        pool_spot_prices,
        pool_safe_prices,
        autopool_owned_shares,
        pool_token_quantity,
        destination_incentive_apr,
        destination_total_apr_in,
        destination_total_apr_out,
        destination_fee_and_base_apr,
    ) = _extract_data(plan)

    token_safe_prices_records.append(token_safe_prices)
    token_spot_prices_records.append(token_spot_prices)
    pool_spot_prices_records.append(pool_spot_prices)
    pool_safe_prices_records.append(pool_safe_prices)
    autopool_owned_shares_records.append(autopool_owned_shares)
    pool_token_quantity_records.append(pool_token_quantity)
    destination_incentive_apr_records.append(destination_incentive_apr)
    destination_total_apr_in_records.append(destination_total_apr_in)
    destination_total_apr_out_records.append(destination_total_apr_out)
    destination_fee_and_base_apr_records.append(destination_fee_and_base_apr)

df_token_safe_prices = pd.DataFrame.from_records(token_safe_prices_records).melt(
    id_vars=["timestamp", "date"], var_name="token", value_name="safe_price"
)


df_pool_spot_prices = pd.DataFrame.from_records(pool_spot_prices_records).melt(
    id_vars=["timestamp", "date"], var_name="pool", value_name="spot_price"
)
df_pool_safe_prices = pd.DataFrame.from_records(pool_safe_prices_records).melt(
    id_vars=["timestamp", "date"], var_name="pool", value_name="safe_price"
)
df_autopool_owned_shares = pd.DataFrame.from_records(autopool_owned_shares_records).melt(
    id_vars=["timestamp", "date"], var_name="pool", value_name="owned_shares"
)
df_destination_incentive_apr = pd.DataFrame.from_records(destination_incentive_apr_records).melt(
    id_vars=["timestamp", "date"], var_name="pool", value_name="incentive_apr"
)
df_destination_total_apr_in = pd.DataFrame.from_records(destination_total_apr_in_records).melt(
    id_vars=["timestamp", "date"], var_name="pool", value_name="total_apr_in"
)
df_destination_total_apr_out = pd.DataFrame.from_records(destination_total_apr_out_records).melt(
    id_vars=["timestamp", "date"], var_name="pool", value_name="total_apr_out"
)
df_destination_fee_and_base_apr = pd.DataFrame.from_records(destination_fee_and_base_apr_records).melt(
    id_vars=["timestamp", "date"], var_name="pool", value_name="fee_and_base_apr"
)


df_token_spot_prices = pd.DataFrame.from_records(token_spot_prices_records)  # not sure
df_pool_token_quantity = pd.DataFrame.from_records(pool_token_quantity_records)  # not sure

df_token_spot_prices = df_token_spot_prices.melt(
    id_vars=["timestamp", "date"], var_name="token", value_name="spot_prices"
)
df_token_spot_prices["pool"] = df_token_spot_prices["token"].apply(lambda x: x[0])
df_token_spot_prices["token"] = df_token_spot_prices["token"].apply(lambda x: x[1])

df_pool_token_quantity = df_pool_token_quantity.melt(
    id_vars=["timestamp", "date"], var_name="token", value_name="quantity"
)
df_pool_token_quantity["pool"] = df_pool_token_quantity["token"].apply(lambda x: x[0])
df_pool_token_quantity["token"] = df_pool_token_quantity["token"].apply(lambda x: x[1])


vault_dfs = [
    df_pool_spot_prices,
    df_pool_safe_prices,
    df_autopool_owned_shares,
    df_destination_incentive_apr,
    df_destination_total_apr_in,
    df_destination_total_apr_out,
    df_destination_fee_and_base_apr,
]

vault_level_data = functools.reduce(
    lambda left, right: pd.merge(left, right, on=["timestamp", "date", "pool"], how="outer"), vault_dfs
)

token_level_data = df_token_safe_prices.copy()

