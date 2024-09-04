import pandas as pd
from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    build_blocks_to_use,
    sync_get_raw_state_by_block_one_block,
)

from v2_rebalance_dashboard.constants import eth_client, balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS, ROOT_DIR
import json
import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

with open(ROOT_DIR / "vault_abi.json", "r") as fin:
    vault_abi = json.load(fin)

with open(ROOT_DIR / "strategy_abi.json", "r") as fin:
    strategy_abi = json.load(fin)

balETH_autopool_vault = "0x72cf6d7C85FfD73F18a83989E7BA8C1c30211b73"
vault_contract = eth_client.eth.contract(balETH_autopool_vault, abi=vault_abi)

autoPool = eth_client.eth.contract(balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS, abi=strategy_abi)


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
            # ignoring slashings costs, no longer part of model
        }
        return summary
    else:
        return None


def build_summary_stats_call(
    name: str,
    autopool_eth_strategy_address: str,
    destination_vault_address: str,
    direction: str = "out",
    amount: int = 0,
) -> Call:

    # hasn't been an error so far
    # /// @notice Gets the safe price of the underlying LP token
    # /// @dev Price validated to be inside our tolerance against spot price. Will revert if outside.
    # /// @return price Value of 1 unit of the underlying LP token in terms of the base asset
    # function getValidatedSafePrice() external returns (uint256 price);

    # getDestinationSummaryStats uses getValidatedSafePrice, it can revert sometimes
    # None, commuicates, uncertaintity, the solver cannot re

    if direction == "in":
        direction_enum = 0
    elif direction == "out":
        direction_enum = 1
    # lose slashing info, intentionally
    return_types = "(address,uint256,uint256,uint256,uint256,int256,int256,int256,uint256,int256,uint256)"

    return Call(
        autopool_eth_strategy_address,
        [
            f"getDestinationSummaryStats(address,uint8,uint256)({return_types})",
            destination_vault_address,
            direction_enum,
            amount,
        ],
        [(name, _clean_summary_stats_info)],
    )


def fetch_summary_stats_figures():
    vaults_df = pd.read_csv(ROOT_DIR / "vaults.csv")
    calls = [
        build_summary_stats_call(
            "idle",
            balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS,
            balETH_autopool_vault,
            direction="out",
            amount=0,
        )
    ]
    for i, (destination_vault_address, vault_name) in enumerate(zip(vaults_df["vaultAddress"], vaults_df["name"])):
        call = build_summary_stats_call(
            name=f"{vault_name}_ {i}",  # some duplicate names here
            autopool_eth_strategy_address=balETH_AUTOPOOL_ETH_STRATEGY_ADDRESS,
            destination_vault_address=destination_vault_address,
            direction="out",
            amount=0,
        )
        calls.append(call)
    blocks = build_blocks_to_use()
    summary_stats_df = sync_safe_get_raw_state_by_block(calls, blocks)
    return summary_stats_df
