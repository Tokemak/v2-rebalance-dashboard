import pandas as pd

from mainnet_launch.pages.solver_diagnostics.solver_diagnostics import (
    _load_solver_df,
    ensure_all_rebalance_plans_are_loaded_from_s3_bucket,
    AUTO_ETH,
    load_solver_plans,
)
import json
import functools

from mainnet_launch.constants import SOLVER_AUGMENTED_REBALANCE_PLANS_DIR


def _get_destination_states(plan: dict):
    if "sod" in plan and "destStates" in plan["sod"]:
        return plan["sod"]["destStates"]
    elif "destStates" in plan:
        return plan["destStates"]
    else:
        raise KeyError("No destStates found in the provided plan JSON.")


# might not be needed
def _set_value_or_raise(dict_to_update: dict, key, value) -> None:
    if key not in dict_to_update:
        dict_to_update[key] = value
    else:
        # sanity check that the same value is the same in each spot
        if dict_to_update[key] != value:
            raise ValueError("unexpected mismatch", f"{dict_to_update=} {dict_to_update[key]=} {value=}")


from dataclasses import dataclass


@dataclass
class PlanData:
    autopool: str
    chain: str
    block: int
    block_timestamp: int
    solver_timestamp: int


@dataclass
class DestinationData:
    destination_vault: str
    name: str
    poolType: str
    pool: str
    underlying: str


@dataclass
class DestinationBlockData:
    plan_data: PlanData
    destination_data: DestinationData

    pool_spot_price: float
    pool_safe_price: float
    pool_backing: float  # can infer the pool backing from the token backing
    autopool_owned_shares: float
    underlying_total_supply: float
    incentive_apr: float
    total_apr_in: float
    total_apr_out: float


    def to_record(self) -> dict:
        # returns a flat dict repersentation of the class
        items = vars(self)
        items.update(vars(items.pop('plan_data')))
        items.update(vars(items.pop('destination_data')))
        return items


@dataclass
class DestinationTokenBlockData:
    plan_data: PlanData
    destination_data: DestinationData

    token_address: str
    token_symbol: str
    safe_price: float
    spot_price: float
    backing: float
    amount: float
    decimals: int

    discountViolationAddFlag: bool
    discountViolationTrim1Flag: bool
    discountViolationTrim2Flag: bool


    def to_record(self) -> dict:
        # returns a flat dict repersentation of the class
        items = vars(self)
        items.update(vars(items.pop('plan_data')))
        items.update(vars(items.pop('destination_data')))
        return items


def _extract_plan_data(rebalance_plan: dict) -> PlanData:
    return PlanData(
        autopool=rebalance_plan["autopool"],
        chain=rebalance_plan["chain"],
        block=rebalance_plan["block"],
        block_timestamp=rebalance_plan["block_timestamp"],
        solver_timestamp=rebalance_plan["mainnet_block_timestamp"],
    )


def _extract_destination_data(dest: dict) -> DestinationData:
    return DestinationData(
        destination_vault=dest["address"],
        name=dest["name"],
        poolType=dest["poolType"],
        pool=dest["pool"],
        underlying=dest["underlying"],
    )


def _extract_destination_block_data(plan_data: PlanData, dest: dict) -> DestinationBlockData:
    total_pool_backing = [
        (amount / 10**tokenDecimals) * backing
        for (amount, tokenDecimals, backing) in zip(
            dest["underlyingTokenAmounts"], dest["tokenDecimals"], dest["tokenBacking"]
        )
    ]
    pool_backing = sum(total_pool_backing) / dest["underlyingTotalSupply"]
    destination_data = _extract_destination_data(dest)

    return DestinationBlockData(
        plan_data=plan_data,
        destination_data=destination_data,
        pool_spot_price=dest["spotPrice"],
        pool_safe_price=dest["safePrice"],
        pool_backing=pool_backing,
        autopool_owned_shares=dest["ownedShares"] / 1e18,
        underlying_total_supply=dest["underlyingTotalSupply"],
        incentive_apr=100 * dest["incentiveAPR"],
        total_apr_in=100 * dest["totalAprIn"],
        total_apr_out=100 * dest["totalAprOut"],
    )


def _extract_destination_token_block_data(plan_data: PlanData, dest: dict) -> list[DestinationTokenBlockData]:

    destination_data = _extract_destination_data(dest)
    destination_token_block_data = []
    for i in range(len(dest["underlyingTokens"])):
        destination_token_block_data.append(
            DestinationTokenBlockData(
                plan_data=plan_data,
                destination_data=destination_data,
                token_address=dest["underlyingTokens"][i],
                token_symbol=dest["underlyingTokenSymbols"][i],
                safe_price=dest["tokenSafePrice"][i],
                spot_price=dest["tokenSpotPrice"][i],
                backing=dest["tokenBacking"][i],
                amount=dest["underlyingTokenAmounts"][i] / (10 ** dest["tokenDecimals"][i]),
                decimals=dest["tokenDecimals"][i],
                discountViolationAddFlag=dest["discountViolationAddFlag"][i],
                discountViolationTrim1Flag=dest["discountViolationTrim1Flag"][i],
                discountViolationTrim2Flag=dest["discountViolationTrim2Flag"][i],
            )
        )

    return destination_token_block_data


def _extract_all_token_plan_data(rebalance_plan_path: str):
    with open(rebalance_plan_path, "r") as fin:
        rebalance_plan = json.load(fin)

        plan_data = _extract_plan_data(rebalance_plan)
        state_of_destinations = rebalance_plan["sod"]["destStates"]
        all_destination_block_data = [
            _extract_destination_block_data(plan_data, dest) for dest in state_of_destinations
        ]

        all_destination_token_block_data = []
        for dest in state_of_destinations:
            all_destination_token_block_data.extend(_extract_destination_token_block_data(plan_data, dest))

        plan_df = pd.DataFrame.from_records([vars(d) for d in [plan_data] ])
        destination_block_df = pd.DataFrame.from_records([d.to_record() for d in all_destination_block_data ])
        destination_token_block_df = pd.DataFrame.from_records([d.to_record() for d in all_destination_token_block_data ])
        return plan_df, destination_block_df, destination_token_block_df

if __name__ == "__main__":
    plans = [p for p in SOLVER_AUGMENTED_REBALANCE_PLANS_DIR.glob("*.json")]

    plan_dfs = []
    destination_block_dfs = []
    destination_token_block_dfs = []

    failed = []
    for p in plans:
        try:
            plan_df, destination_block_df, destination_token_block_df = _extract_all_token_plan_data(p)
            plan_dfs.append(plan_df)
            destination_block_dfs.append(destination_block_df)
            destination_token_block_dfs.append(destination_token_block_df)
        except Exception as e:
            failed.append(p)
    

    plan_df = pd.concat(plan_dfs, axis=0)
    destination_block_df = pd.concat(destination_block_dfs, axis=0)
    destination_token_block_df = pd.concat(destination_token_block_dfs, axis=0)
    print(plan_df.shape)
    from mainnet_launch.constants import WORKING_DATA_DIR

    plan_df.to_parquet('working_data/plan_df.parquet')
    destination_block_df.to_parquet('working_data/destination_block_df.parquet')
    destination_token_block_df.to_parquet('working_data/destination_token_block_df.parquet')

    print(destination_block_df.max())
    print(destination_token_block_df.max())
    print(destination_token_block_df.shape)


    pass


#     """
#     extract out the by destination level data for each autopool state

#     (lptoken, token) spot price

#     (token) safe price

#     (lptoken) spot price

#     (lptoken) safe price

#     (lptoken) autopool owned shares

#     (lptoken, token) pool quantity


#     # apr data

#     incentive, total apr in, total apr out,

#     """
#     date_info = {"timestamp": plan["timestamp"], "date": pd.to_datetime(plan["timestamp"], unit="s", utc=True)}
#     (
#         token_safe_prices,
#         token_spot_prices,
#         pool_spot_prices,
#         pool_safe_prices,
#         autopool_owned_shares,
#         pool_token_quantity,
#         destination_incentive_apr,
#         destination_total_apr_in,
#         destination_total_apr_out,
#         destination_fee_and_base_apr,
#     ) = [date_info.copy() for _ in range(10)]

#     def _extract_price_information_from_solver(dest: dict):

#         if "decimals" not in dest:
#             decimals = [18 for _ in dest["underlyingTokens"]]
#         else:
#             decimals = dest["decimals"]
#         for i in range(len(decimals)):
#             token_address = dest["underlyingTokens"][i]
#             underlying_lp_token_address = dest["underlying"]

#             _set_value_or_raise(token_safe_prices, token_address, dest["tokenSafePrice"][i])
#             _set_value_or_raise(
#                 token_spot_prices, (underlying_lp_token_address, token_address), dest["tokenSpotPrice"][i]
#             )

#             _set_value_or_raise(pool_spot_prices, underlying_lp_token_address, dest["spotPrice"])
#             _set_value_or_raise(pool_safe_prices, underlying_lp_token_address, dest["safePrice"])

#             _set_value_or_raise(autopool_owned_shares, underlying_lp_token_address, dest["ownedShares"] / 1e18)
#             _set_value_or_raise(
#                 pool_token_quantity,
#                 (underlying_lp_token_address, token_address),
#                 dest["underlyingTokenAmounts"][i] / 10 ** decimals[i],
#             )

#             # apr data
#             _set_value_or_raise(destination_incentive_apr, underlying_lp_token_address, 100 * dest["incentiveAPR"])
#             _set_value_or_raise(destination_total_apr_in, underlying_lp_token_address, 100 * dest["totalAprIn"])
#             _set_value_or_raise(destination_total_apr_out, underlying_lp_token_address, 100 * dest["totalAprOut"])

#             _set_value_or_raise(
#                 destination_fee_and_base_apr,
#                 underlying_lp_token_address,
#                 100 * (dest["totalAprOut"] - dest["incentiveAPR"]),
#             )

#     for dest in _get_destination_states(plan):
#         _extract_price_information_from_solver(dest)

#     return (
#         token_safe_prices,
#         token_spot_prices,
#         pool_spot_prices,
#         pool_safe_prices,
#         autopool_owned_shares,
#         pool_token_quantity,
#         destination_incentive_apr,
#         destination_total_apr_in,
#         destination_total_apr_out,
#         destination_fee_and_base_apr,
#     )


# def _combine_plan_info(all_plans: list[str]):
#     token_safe_prices_records = []
#     token_spot_prices_records = []
#     pool_spot_prices_records = []
#     pool_safe_prices_records = []
#     autopool_owned_shares_records = []
#     pool_token_quantity_records = []
#     destination_incentive_apr_records = []
#     destination_total_apr_in_records = []
#     destination_total_apr_out_records = []
#     destination_fee_and_base_apr_records = []

#     for plan in autoETH_plans:
#         (
#             token_safe_prices,
#             token_spot_prices,
#             pool_spot_prices,
#             pool_safe_prices,
#             autopool_owned_shares,
#             pool_token_quantity,
#             destination_incentive_apr,
#             destination_total_apr_in,
#             destination_total_apr_out,
#             destination_fee_and_base_apr,
#         ) = _extract_data(plan)

#         token_safe_prices_records.append(token_safe_prices)
#         token_spot_prices_records.append(token_spot_prices)
#         pool_spot_prices_records.append(pool_spot_prices)
#         pool_safe_prices_records.append(pool_safe_prices)
#         autopool_owned_shares_records.append(autopool_owned_shares)
#         pool_token_quantity_records.append(pool_token_quantity)
#         destination_incentive_apr_records.append(destination_incentive_apr)
#         destination_total_apr_in_records.append(destination_total_apr_in)
#         destination_total_apr_out_records.append(destination_total_apr_out)
#         destination_fee_and_base_apr_records.append(destination_fee_and_base_apr)

#     df_token_safe_prices = pd.DataFrame.from_records(token_safe_prices_records).melt(
#         id_vars=["timestamp", "date"], var_name="token", value_name="safe_price"
#     )

#     df_pool_spot_prices = pd.DataFrame.from_records(pool_spot_prices_records).melt(
#         id_vars=["timestamp", "date"], var_name="pool", value_name="spot_price"
#     )
#     df_pool_safe_prices = pd.DataFrame.from_records(pool_safe_prices_records).melt(
#         id_vars=["timestamp", "date"], var_name="pool", value_name="safe_price"
#     )
#     df_autopool_owned_shares = pd.DataFrame.from_records(autopool_owned_shares_records).melt(
#         id_vars=["timestamp", "date"], var_name="pool", value_name="owned_shares"
#     )
#     df_destination_incentive_apr = pd.DataFrame.from_records(destination_incentive_apr_records).melt(
#         id_vars=["timestamp", "date"], var_name="pool", value_name="incentive_apr"
#     )
#     df_destination_total_apr_in = pd.DataFrame.from_records(destination_total_apr_in_records).melt(
#         id_vars=["timestamp", "date"], var_name="pool", value_name="total_apr_in"
#     )
#     df_destination_total_apr_out = pd.DataFrame.from_records(destination_total_apr_out_records).melt(
#         id_vars=["timestamp", "date"], var_name="pool", value_name="total_apr_out"
#     )
#     df_destination_fee_and_base_apr = pd.DataFrame.from_records(destination_fee_and_base_apr_records).melt(
#         id_vars=["timestamp", "date"], var_name="pool", value_name="fee_and_base_apr"
#     )

#     df_token_spot_prices = pd.DataFrame.from_records(token_spot_prices_records)  # not sure
#     df_pool_token_quantity = pd.DataFrame.from_records(pool_token_quantity_records)  # not sure

#     df_token_spot_prices = df_token_spot_prices.melt(
#         id_vars=["timestamp", "date"], var_name="token", value_name="spot_prices"
#     )
#     df_token_spot_prices["pool"] = df_token_spot_prices["token"].apply(lambda x: x[0])
#     df_token_spot_prices["token"] = df_token_spot_prices["token"].apply(lambda x: x[1])

#     df_pool_token_quantity = df_pool_token_quantity.melt(
#         id_vars=["timestamp", "date"], var_name="token", value_name="quantity"
#     )
#     df_pool_token_quantity["pool"] = df_pool_token_quantity["token"].apply(lambda x: x[0])
#     df_pool_token_quantity["token"] = df_pool_token_quantity["token"].apply(lambda x: x[1])

#     vault_dfs = [
#         df_pool_spot_prices,
#         df_pool_safe_prices,
#         df_autopool_owned_shares,
#         df_destination_incentive_apr,
#         df_destination_total_apr_in,
#         df_destination_total_apr_out,
#         df_destination_fee_and_base_apr,
#     ]

#     vault_level_data = functools.reduce(
#         lambda left, right: pd.merge(left, right, on=["timestamp", "date", "pool"], how="outer"), vault_dfs
#     )

#     token_level_data = df_token_safe_prices.copy()


# # maybe not needed
# @dataclass
# class TokenBlockData:
#     # (token, block) -> value
#     destination_vault_address: str
#     autopool: str
#     chain: str
#     block: int
#     block_timestamp: int
#     solver_timestamp: int
#     # peg_asset: str  # one of ['USDC', 'WETH']
#     token_address: str
#     token_symbol: str
#     safe_price: float
#     backing: float
