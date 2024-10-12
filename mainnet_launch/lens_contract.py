from multicall import Call
import streamlit as st
import pandas as pd
from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
    identity_function,
    build_blocks_to_use,
    get_raw_state_by_blocks,
)
from mainnet_launch.abis.abis import LENS_CONTRACT_ABI
from mainnet_launch.constants import CACHE_TIME, ALL_AUTOPOOLS, eth_client, LENS_CONTRACT

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
        "totalSupply": autopool_data[13],
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


def get_pools_and_destinations_call() -> Call:
    return Call(
        LENS_CONTRACT,
        [GET_POOLS_AND_DESTINATIONS_SIGNATURE],
        [["getPoolsAndDestinations", _handle_get_pools_and_destinations]],
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
        raise ValueError(f'direction can only be `in` or `out` is {direction=}')

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


def fetch_pools_and_destinations_df() -> pd.DataFrame:
    blocks = build_blocks_to_use()
    calls = [get_pools_and_destinations_call()]
    pools_and_destinations_df = get_raw_state_by_blocks(calls, blocks)
    return pools_and_destinations_df

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
