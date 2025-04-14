from dataclasses import dataclass


import json
import pandas as pd

from mainnet_launch.constants import SOLVER_AUGMENTED_REBALANCE_PLANS_DIR, AutopoolConstants, time_decorator
from mainnet_launch.database.database_operations import write_dataframe_to_table, run_read_only_query, drop_table

REBALANCE_PLAN_TABLE = "REBALANCE_PLAN_TABLE"
DESTINATION_BLOCK_TABLE = "DESTINATION_BLOCK_DATA_TABLE"
DESTINATION_TOKEN_BLOCK_TABLE = "DESTINATION_TOKEN_BLOCK_TABLE"


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
    vault_name: str
    poolType: str
    pool: str
    underlying: str


@dataclass
class DestinationBlockData:
    plan_data: PlanData
    destination_data: DestinationData
    pool_spot_price: float
    pool_safe_price: float
    pool_backing: float
    autopool_owned_shares: float
    underlying_total_supply: float
    incentive_apr: float
    total_apr_in: float
    total_apr_out: float

    def to_record(self) -> dict:
        # returns a flat dict
        items = vars(self)
        items.update(vars(items.pop("plan_data")))
        items.update(vars(items.pop("destination_data")))
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
    amount: float  # total amount in the pool, not just ours
    decimals: int

    discountViolationAddFlag: bool
    discountViolationTrim1Flag: bool
    discountViolationTrim2Flag: bool

    # not certain this is the right place for it
    portion_ownership: float

    def to_record(self) -> dict:
        # returns a flat dict repersentation of the class
        items = vars(self)
        items.update(vars(items.pop("plan_data")))
        items.update(vars(items.pop("destination_data")))
        return items


def _extract_plan_data(rebalance_plan: dict) -> PlanData:
    return PlanData(
        autopool=rebalance_plan["autopool"],
        chain=rebalance_plan["chain"],
        block=rebalance_plan["block"],
        block_timestamp=rebalance_plan["block_timestamp"],
        solver_timestamp=rebalance_plan["mainnet_block_timestamp"],
        # TODO switch over here in the database file
        # block_timestamp=pd.to_datetime(rebalance_plan["block_timestamp"], unit='s', utc=True),
        # solver_timestamp=pd.to_datetime(rebalance_plan["mainnet_block_timestamp"], unit='s', utc=True)
    )


def _extract_destination_data(dest: dict) -> DestinationData:
    return DestinationData(
        destination_vault=dest["address"],
        vault_name=dest["name"].replace("Tokemak-Wrapped Ether-", ""),
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
        incentive_apr=dest["incentiveAPR"],
        total_apr_in=dest["totalAprIn"],
        total_apr_out=dest["totalAprOut"],
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
                portion_ownership=(dest["ownedShares"] / 1e18) / dest["underlyingTotalSupply"],
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

        plan_df = pd.DataFrame.from_records([vars(d) for d in [plan_data]])
        destination_block_df = pd.DataFrame.from_records([d.to_record() for d in all_destination_block_data])
        destination_token_block_df = pd.DataFrame.from_records(
            [d.to_record() for d in all_destination_token_block_data]
        )
        return plan_df, destination_block_df, destination_token_block_df


@time_decorator
def update_rebalance_plan_tables():
    plans = [p for p in SOLVER_AUGMENTED_REBALANCE_PLANS_DIR.glob("*.json")]

    plan_dfs = []
    destination_block_dfs = []
    destination_token_block_dfs = []

    failed = []
    for p in plans:
        try:
            # too slow
            plan_df, destination_block_df, destination_token_block_df = _extract_all_token_plan_data(p)
            plan_dfs.append(plan_df)
            destination_block_dfs.append(destination_block_df)
            destination_token_block_dfs.append(destination_token_block_df)
        except Exception as e:
            failed.append(p)

    plan_df = pd.concat(plan_dfs, axis=0)
    destination_block_df = pd.concat(destination_block_dfs, axis=0)
    destination_token_block_df = pd.concat(destination_token_block_dfs, axis=0)

    # write_dataframe_to_table(plan_df, REBALANCE_PLAN_TABLE)
    write_dataframe_to_table(destination_block_df, DESTINATION_BLOCK_TABLE)
    write_dataframe_to_table(destination_token_block_df, DESTINATION_TOKEN_BLOCK_TABLE)


def fetch_destination_summary_stats2(autopool: AutopoolConstants, summary_stats_field: str):
    # drop_table(DESTINATION_BLOCK_TABLE)
    # # drop_table(REBALANCE_PLAN_TABLE)
    # drop_table(DESTINATION_TOKEN_BLOCK_TABLE)
    update_rebalance_plan_tables()
    # raise ValueError('s')

    query = f"""
        SELECT vault_name, block_timestamp, {summary_stats_field} from {DESTINATION_BLOCK_TABLE}
        WHERE autopool = ?
        """
    params = (autopool.name,)
    long_summary_stats_df = run_read_only_query(query, params)
    summary_stats_df = pd.pivot(
        long_summary_stats_df, columns="vault_name", values=summary_stats_field, index="block_timestamp"
    )
    return summary_stats_df


if __name__ == "__main__":

    from mainnet_launch.constants import AUTO_ETH

    drop_table(DESTINATION_TOKEN_BLOCK_TABLE)

    a = fetch_destination_summary_stats2(AUTO_ETH, "underlying_total_supply")
    print(a.tail())

    print(a.tail(1).T)
    pass
