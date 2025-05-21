from multicall import Call
import pandas as pd
from web3 import Web3

from mainnet_launch.data_fetching.get_state_by_block import (
    get_raw_state_by_blocks,
)
from mainnet_launch.constants import LENS_CONTRACT, ChainData
from mainnet_launch.database.schema.full import (
    Autopools,
    Destinations,
)
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_orm,
)

block_number = 20929842


GET_POOLS_AND_DESTINATIONS_SIGNATURE = "getPoolsAndDestinations()(((address,string,string,bytes32,address,uint256,uint256,bool,bool,bool,uint8,address,address,uint256,uint256,uint256,uint256,uint256)[],(address,string,uint256,uint256,uint256,uint256,uint256,uint256,uint256,bool,bool,bool,uint8,uint256,uint256,address,address,string,string,uint256,uint8,int256,(address)[],(address)[],(string)[],(uint256,uint256,int256,uint24[10],uint40)[],(uint256)[],uint256[],uint40[],uint256[])[][]))"


def parse_autopool(autopool_data):
    return {
        "poolAddress": autopool_data[0],
        "name": autopool_data[1],
        "symbol": autopool_data[2],
        "vaultType": autopool_data[3],
        "baseAsset": autopool_data[4],
        "streamingFeeBps": autopool_data[5],
        "periodicFeeBps": autopool_data[6],
        "feeHighMarkEnabled": autopool_data[7],
        "feeSettingsIncomplete": autopool_data[8],
        "isShutdown": autopool_data[9],
        "shutdownStatus": autopool_data[10],
        "rewarder": autopool_data[11],
        "strategy": autopool_data[12],
        "totalSupply": autopool_data[13],  # useful for the autopool state
        "totalAssets": autopool_data[14],
        "totalIdle": autopool_data[15],
        "totalDebt": autopool_data[16],
        "navPerShare": autopool_data[17],
    }


def parse_reward_tokens(reward_tokens_data):
    return [{"tokenAddress": token[0]} for token in reward_tokens_data]


def parse_underlying_tokens(underlying_tokens_data):
    return [{"tokenAddress": token[0]} for token in underlying_tokens_data]


def parse_underlying_token_symbols(underlying_token_symbols_data):
    return [{"symbol": symbol[0]} for symbol in underlying_token_symbols_data]


def parse_lst_stats_data(lst_stats_data):
    return [
        {
            "lastSnapshotTimestamp": stats[0],
            "baseApr": stats[1],
            "discount": stats[2],
            "discountHistory": stats[3],
            "discountTimestampByPercent": stats[4],
        }
        for stats in lst_stats_data
    ]


def parse_underlying_token_value_held(underlying_token_value_held_data):
    return [{"valueHeldInEth": value[0]} for value in underlying_token_value_held_data]


def parse_destination_vault(vault_data):
    return {
        "vaultAddress": vault_data[0],
        "exchangeName": vault_data[1],
        "totalSupply": vault_data[2],
        "lastSnapshotTimestamp": vault_data[3],
        "feeApr": vault_data[4],
        "lastDebtReportTime": vault_data[5],
        "minDebtValue": vault_data[6],
        "maxDebtValue": vault_data[7],
        "debtValueHeldByVault": vault_data[8],
        "queuedForRemoval": vault_data[9],
        "statsIncomplete": vault_data[10],
        "isShutdown": vault_data[11],
        "shutdownStatus": vault_data[12],
        "autoPoolOwnsShares": vault_data[13],
        "actualLPTotalSupply": vault_data[14],
        "dexPool": vault_data[15],
        "lpTokenAddress": vault_data[16],
        "lpTokenSymbol": vault_data[17],
        "lpTokenName": vault_data[18],
        "statsSafeLPTotalSupply": vault_data[19],
        "statsIncentiveCredits": vault_data[20],
        "compositeReturn": vault_data[21],
        "rewardsTokens": parse_reward_tokens(vault_data[22]),
        "underlyingTokens": parse_underlying_tokens(vault_data[23]),
        "underlyingTokenSymbols": parse_underlying_token_symbols(vault_data[24]),
        "lstStatsData": parse_lst_stats_data(vault_data[25]),
        "underlyingTokenValueHeld": parse_underlying_token_value_held(vault_data[26]),
        "reservesInEth": vault_data[27],
        "statsPeriodFinishForRewards": vault_data[28],
        "statsAnnualizedRewardAmounts": vault_data[29],
    }


def _handle_get_pools_and_destinations(success, response):
    if success:
        autopools_data, destinations_data = response

        parsed_autopools = [parse_autopool(autopool) for autopool in autopools_data]

        parsed_destinations = [
            [parse_destination_vault(vault) for vault in destination_list] for destination_list in destinations_data
        ]

        return {"autopools": parsed_autopools, "destinations": parsed_destinations}


def get_pools_and_destinations_call(chain: ChainData) -> Call:
    return Call(
        LENS_CONTRACT(chain),
        [GET_POOLS_AND_DESTINATIONS_SIGNATURE],
        [["getPoolsAndDestinations", _handle_get_pools_and_destinations]],
    )


def _extract_only_autopools_and_destinations(success, response) -> dict:
    if success:
        autopools_data, destinations_data = response

        autopool_vault_address = [a[0] for a in autopools_data]
        destination_vault_addresses = []
        for destinations_list in destinations_data:
            destination_vault_addresses.append([Web3.toChecksumAddress(d[0]) for d in destinations_list])
        return {Web3.toChecksumAddress(a): d for a, d in zip(autopool_vault_address, destination_vault_addresses)}


def get_pools_and_destinations_call_only_autopools_and_destinations(chain: ChainData) -> Call:
    return Call(
        LENS_CONTRACT(chain),
        [GET_POOLS_AND_DESTINATIONS_SIGNATURE],
        [["getPoolsAndDestinations", _extract_only_autopools_and_destinations]],
    )


def _clean_summary_stats_info(success, summary_stats):
    if success is True:
        summary = {
            "destination": summary_stats[0],
            "baseApr": summary_stats[1] / 1e18,
            "feeApr": summary_stats[2] / 1e18,
            "incentiveApr": summary_stats[3] / 1e18,
            "safeTotalSupply": summary_stats[4] / 1e18,
            "priceReturn": summary_stats[5] / 1e18,
            "maxDiscount": summary_stats[6] / 1e18,
            "maxPremium": summary_stats[7] / 1e18,
            "ownedShares": summary_stats[8] / 1e18,
            "compositeReturn": summary_stats[9] / 1e18,
            "pricePerShare": summary_stats[10] / 1e18,
        }
        return summary
    else:
        return None


def build_proxyGetDestinationSummaryStats_call(
    name: str, autopool_strategy_address: str, destination_address: str, direction: str, amount: int
) -> Call:
    if direction == "in":
        direction_enum = 0
    elif direction == "out":
        direction_enum = 1
    else:
        raise ValueError(f"direction can only be `in` or `out` is {direction=}")

    return Call(
        LENS_CONTRACT,
        [
            f"proxyGetDestinationSummaryStats(address,address,uint8,uint256)((address,uint256,uint256,uint256,uint256,int256,int256,int256,uint256,int256,uint256))",
            autopool_strategy_address,
            destination_address,
            direction_enum,
            amount,
        ],
        [(name, _clean_summary_stats_info)],
    )


def fetch_pools_and_destinations_df(chain: ChainData, blocks: list[int]) -> pd.DataFrame:
    calls = [get_pools_and_destinations_call(chain)]
    pools_and_destinations_df = get_raw_state_by_blocks(calls, blocks, chain=chain, include_block_number=True)
    return pools_and_destinations_df


def fetch_active_destinations_by_autopool_by_block(chain: ChainData, blocks: list[int]) -> pd.DataFrame:
    calls = [get_pools_and_destinations_call_only_autopools_and_destinations(chain)]
    pools_and_destinations_df = get_raw_state_by_blocks(calls, blocks, chain=chain, include_block_number=True)
    return pools_and_destinations_df


# maybe not the best spot for this, else where?
def fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks(
    chain: ChainData, missing_blocks: list[int]
) -> dict[str, list[Destinations]]:
    all_destinations_orm: list[Destinations] = get_full_table_as_orm(
        Destinations, where_clause=Destinations.chain_id == chain.chain_id
    )
    all_autopools_orm: list[Autopools] = get_full_table_as_orm(
        Autopools, where_clause=Autopools.chain_id == chain.chain_id
    )

    raw_df = fetch_active_destinations_by_autopool_by_block(chain, missing_blocks)

    active_destinations_by_autopool_df = pd.DataFrame.from_records(raw_df["getPoolsAndDestinations"].values)
    # make a bunch of summary stats calls
    # split up by autopools to avoid max gas costs
    autopool_to_all_ever_active_destinations: dict[str | list[Destinations]] = {}
    for autopool in all_autopools_orm:
        this_autopool_destinations = set()
        all_ever_active_destinations = (
            active_destinations_by_autopool_df[autopool.autopool_vault_address].dropna().values
        )
        for active_destinations_at_this_block in all_ever_active_destinations:
            this_autopool_destinations.update(active_destinations_at_this_block)

        autopool_to_all_ever_active_destinations[autopool.autopool_vault_address] = [
            d for d in all_destinations_orm if d.destination_vault_address in this_autopool_destinations
        ]
    return autopool_to_all_ever_active_destinations


def fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks_address(
    chain: ChainData, missing_blocks: list[int]
) -> dict[str, list[str]]:
    all_autopools_orm: list[Autopools] = get_full_table_as_orm(
        Autopools, where_clause=Autopools.chain_id == chain.chain_id
    )

    raw_df = fetch_active_destinations_by_autopool_by_block(chain, missing_blocks)

    active_destinations_by_autopool_df = pd.DataFrame.from_records(raw_df["getPoolsAndDestinations"].values)
    # make a bunch of summary stats calls
    # split up by autopools to avoid max gas costs
    autopool_to_all_ever_active_destinations: dict[str | list[Destinations]] = {}
    for autopool in all_autopools_orm:
        this_autopool_destinations = set()
        all_ever_active_destinations = (
            active_destinations_by_autopool_df[autopool.autopool_vault_address].dropna().values
        )
        for active_destinations_at_this_block in all_ever_active_destinations:
            this_autopool_destinations.update(active_destinations_at_this_block)

        this_autopool_destinations = [
            Web3.toChecksumAddress(destination_vault_address)
            for destination_vault_address in this_autopool_destinations
        ]
        autopool_to_all_ever_active_destinations[autopool.autopool_vault_address] = this_autopool_destinations
    return autopool_to_all_ever_active_destinations


if __name__ == "__main__":

    from mainnet_launch.constants import ETH_CHAIN

    df = fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks_address(
        ETH_CHAIN, [22448783, 22348783]
    )

    # # Process and return results

    # struct Autopool {
    #     address poolAddress;
    #     string name;
    #     string symbol;
    #     bytes32 vaultType;
    #     address baseAsset;
    #     uint256 streamingFeeBps;
    #     uint256 periodicFeeBps;
    #     bool feeHighMarkEnabled;
    #     bool feeSettingsIncomplete;
    #     bool isShutdown;
    #     IAutopool.VaultShutdownStatus shutdownStatus;
    #     address rewarder;
    #     address strategy;
    #     uint256 totalSupply;
    #     uint256 totalAssets;
    #     uint256 totalIdle;
    #     uint256 totalDebt;
    #     uint256 navPerShare;
    # }

    # struct RewardToken {
    #     address tokenAddress;
    # }

    # struct TokenAmount {
    #     uint256 amount;
    # }

    # struct UnderlyingTokenValueHeld {
    #     uint256 valueHeldInEth;
    # }

    # struct UnderlyingTokenAddress {
    #     address tokenAddress;
    # }

    # struct UnderlyingTokenSymbol {
    #     string symbol;
    # }

    # struct DestinationVault {
    #     address vaultAddress;
    #     string exchangeName;
    #     uint256 totalSupply;
    #     uint256 lastSnapshotTimestamp;
    #     uint256 feeApr;
    #     uint256 lastDebtReportTime;
    #     uint256 minDebtValue;
    #     uint256 maxDebtValue;
    #     uint256 debtValueHeldByVault;
    #     bool queuedForRemoval;
    #     bool statsIncomplete;
    #     bool isShutdown;
    #     IDestinationVault.VaultShutdownStatus shutdownStatus;
    #     uint256 autoPoolOwnsShares;
    #     uint256 actualLPTotalSupply;
    #     address dexPool;
    #     address lpTokenAddress;
    #     string lpTokenSymbol;
    #     string lpTokenName;
    #     uint256 statsSafeLPTotalSupply;
    #     uint8 statsIncentiveCredits;
    #     int256 compositeReturn;
    #     RewardToken[] rewardsTokens;
    #     UnderlyingTokenAddress[] underlyingTokens;
    #     UnderlyingTokenSymbol[] underlyingTokenSymbols;
    #     ILSTStats.LSTStatsData[] lstStatsData;
    #     UnderlyingTokenValueHeld[] underlyingTokenValueHeld;
    #     uint256[] reservesInEth;
    #     uint40[] statsPeriodFinishForRewards;
    #     uint256[] statsAnnualizedRewardAmounts;
    # }

    # struct Autopools {
    #     Autopool[] autoPools;
    #     DestinationVault[][] destinations;
    # }


# if __name__ == "__main__":


#     a = df.values[0]
#     pass
