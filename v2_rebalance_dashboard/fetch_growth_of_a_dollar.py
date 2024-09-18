from multicall import Call
import plotly.graph_objects as go
import streamlit as st

import pandas as pd


from v2_rebalance_dashboard.constants import (
    ROOT_DIR,
    eth_client,
    BALANCER_AURA_DESTINATION_VAULT_ABI,
    balETH_AUTOPOOL_ETH_ADDRESS,
    ROOT_PRICE_ORACLE,
    EXTRA_REWARD_POOL_ABI,
    ERC_20_ABI,
    BASE_REWARD_POOL_ABI,
    AURA_STASH_TOKEN_ABI,
)

from v2_rebalance_dashboard.get_state_by_block import (
    safe_normalize_with_bool_success,
    sync_get_raw_state_by_block_one_block,
    sync_safe_get_raw_state_by_block,
    build_blocks_to_use,
    safe_normalize_6_with_bool_success,
)

GROWTH_OF_A_DOLLAR_START_DATE = "2024, 7, 17"
AURA = "0xC0c293ce456fF0ED870ADd98a0828Dd4d2903DBF"
BAL = "0xba100000625a3754423978a60c9317c58a424e3D"


def get_required_addresses_for_balancer_growth_of_a_dollar(row: dict):
    destination_vault_address = row["vaultAddress"]

    try:
        BalancerAuraDestinationVault = eth_client.eth.contract(
            eth_client.toChecksumAddress(destination_vault_address), abi=BALANCER_AURA_DESTINATION_VAULT_ABI
        )
        aura_incentive_stats_address = BalancerAuraDestinationVault.functions.getStats().call()
        auraStaking_address = BalancerAuraDestinationVault.functions.auraStaking().call()

        auraStaking_contract = eth_client.eth.contract(auraStaking_address, abi=BASE_REWARD_POOL_ABI)
        num_extra_rewards = (
            auraStaking_contract.functions.extraRewardsLength().call()
        )  # migh miss things if rewarders are removed at the end.

        extra_rewarder_details = []

        for i in range(num_extra_rewards):
            extra_rewarder_address = auraStaking_contract.functions.extraRewards(i).call()

            extra_reward_token_address = (
                eth_client.eth.contract(extra_rewarder_address, abi=EXTRA_REWARD_POOL_ABI)
                .functions.rewardToken()
                .call()
            )
            extra_reward_token_symbol = (
                eth_client.eth.contract(extra_reward_token_address, abi=ERC_20_ABI).functions.symbol().call()
            )
            extra_reward_token_decimals = (
                eth_client.eth.contract(extra_reward_token_address, abi=ERC_20_ABI).functions.symbol().call()
            )
            if "STASH-" == extra_reward_token_symbol[:6]:
                base_token_address = (
                    eth_client.eth.contract(extra_reward_token_address, abi=AURA_STASH_TOKEN_ABI)
                    .functions.baseToken()
                    .call()
                )
                base_token_symbol = (
                    eth_client.eth.contract(base_token_address, abi=ERC_20_ABI).functions.symbol().call()
                )
            else:
                base_token_address = extra_reward_token_address
                base_token_symbol = extra_reward_token_symbol

            extra_reward_token_decimals = (
                eth_client.eth.contract(base_token_address, abi=ERC_20_ABI).functions.decimals().call()
            )

            extra_rewarder_details.append(
                {
                    "extra_reward_token_address": extra_reward_token_address,
                    "extra_reward_token_symbol": extra_reward_token_symbol,
                    "base_token_address": base_token_address,
                    "base_token_symbol": base_token_symbol,
                    "extra_rewarder_address": extra_rewarder_address,
                    "extra_reward_token_decimals": extra_reward_token_decimals,
                    "auraStaking_address": auraStaking_address,
                }
            )

        return {
            "destination_vault_address": destination_vault_address,
            "aura_incentive_stats_address": aura_incentive_stats_address,
            "auraStaking_address": auraStaking_address,
            "destinationName": row["name"][22:],
            "extra_rewarder_details": extra_rewarder_details,
        }
    except Exception as e:
        return {"destination_vault_address": destination_vault_address, "error": str(e) + str(type(e))}


def _fetch_required_addresses():
    vault_df = pd.read_csv(ROOT_DIR / "vaults.csv")
    df = pd.DataFrame.from_records(
        vault_df.apply(get_required_addresses_for_balancer_growth_of_a_dollar, axis=1)
    )  # 30 seconds
    bal_rewarders_df = df[df["error"].isna()].copy()

    bal_rewarders_df["extraRewardTokens"] = bal_rewarders_df["extra_rewarder_details"].apply(
        lambda details: [d["base_token_address"] for d in details]
    )
    bal_rewarders_df["extraRewardersRewardTokenSymbol"] = bal_rewarders_df["extra_rewarder_details"].apply(
        lambda details: [d["base_token_symbol"] for d in details]
    )
    bal_rewarders_df["extraRewardersRewardTokenDecimals"] = bal_rewarders_df["extra_rewarder_details"].apply(
        lambda details: [d["extra_reward_token_decimals"] for d in details]
    )
    bal_rewarders_df["extraRewarders"] = bal_rewarders_df["extra_rewarder_details"].apply(
        lambda details: [d["extra_rewarder_address"] for d in details]
    )

    return bal_rewarders_df


def _get_safe_price_from_getRangePricesLP(success, value):
    if success:
        spotPriceInQuote, safePriceInQuote, isSpotSafe = value
        return int(safePriceInQuote) / 1e18


def _build_price_calls(rewardTokens: list[str], rewardTokenSymbols: list[str]) -> list[Call]:
    """Returns a list of calls that get the safe ETH value in ETH for each reward token"""
    price_calls = []

    for rewardToken, symbol in zip(rewardTokens, rewardTokenSymbols):
        price_calls.append(
            Call(
                ROOT_PRICE_ORACLE,
                ["getPriceInEth(address)(uint256)", rewardToken],
                [(f"{symbol}_to_ETH", safe_normalize_with_bool_success)],
            )
        )
    return price_calls


def _build_rewardPerToken_calls(
    mainRewarder: str,
    extraRewarders: list[str],
    extraRewardersRewardTokenSymbol: list[str],
    extraRewardersRewardTokenDecimals: list[int],
) -> list[Call]:
    """Get the rewardPerToken for each of the rewarder"""
    bal_rewards_call = Call(
        mainRewarder,
        ["rewardPerToken()(uint256)"],
        [("mintedBAL_rewardPerToken", safe_normalize_with_bool_success)],
    )

    aura_minted_from_one_BAL_call = Call(
        "0x551050d2dB5043b70598B148e83c9ca16fa21B10",  # just has to be an incentive stats contract for AURA-BAL this is an old one
        [
            "getPlatformTokenMintAmount(address,uint256)(uint256)",
            AURA,
            int(1e18),
        ],  # imo rounding error, 1.0936 to 1.0696 in a 50 days 1:1 would be 95% as close
        [("AURA_minted_for_one_BAL", safe_normalize_with_bool_success)],
    )

    rewardPerTokenCalls = []
    for i, (extraRewarder, extraRewardTokenSymbol, decimals) in enumerate(
        zip(extraRewarders, extraRewardersRewardTokenSymbol, extraRewardersRewardTokenDecimals)
    ):
        if decimals == 18:
            func = safe_normalize_with_bool_success
        elif decimals == 6:
            func = safe_normalize_6_with_bool_success
        rewardPerTokenCall = Call(
            extraRewarder,
            ["rewardPerToken()(uint256)"],
            [(f"{extraRewardTokenSymbol}|| {i} extraRewardPerToken", func)],  # using `i` here to avoid duplicates
        )
        rewardPerTokenCalls.append(rewardPerTokenCall)

    return [bal_rewards_call, aura_minted_from_one_BAL_call, *rewardPerTokenCalls]


def build_growth_of_a_dollar_calls(
    rewardTokens: list[str],
    rewardTokenSymbols: list[str],
    mainRewarder: str,
    extraRewarders: list[str],
    extraRewardersRewardTokenSymbols: list[str],
    extraRewardersRewardTokenDecimals: list[int],
    balancerAuraDestinationVault_address: str,
) -> list[Call]:
    price_calls = _build_price_calls(rewardTokens, rewardTokenSymbols)
    reward_per_token_calls = _build_rewardPerToken_calls(
        mainRewarder,
        extraRewarders,
        extraRewardersRewardTokenSymbols,
        extraRewardersRewardTokenDecimals,
    )

    get_safe_lp_token_value_call = Call(
        balancerAuraDestinationVault_address,
        ["getRangePricesLP()((uint256,uint256,bool))"],
        [("safeLPTokenPriceInETH", _get_safe_price_from_getRangePricesLP)],
    )

    return [*price_calls, *reward_per_token_calls, get_safe_lp_token_value_call]


def transform_growth_of_a_dollar_df_to_growth_of_1_ETH_with_no_entrance_costs(
    raw_growth_of_a_dollar_df: pd.DataFrame, mock_date_deployed: str = "2024, 7, 17"
):
    growth_of_a_dollar_df = raw_growth_of_a_dollar_df[raw_growth_of_a_dollar_df.index > mock_date_deployed].copy()
    growth_of_a_dollar_df["starting_quantity_of_lp_tokens"] = (
        1 / growth_of_a_dollar_df["safeLPTokenPriceInETH"].values[0]
    )  # constant
    growth_of_a_dollar_df["safe_value_of_lp_tokens"] = (
        growth_of_a_dollar_df["starting_quantity_of_lp_tokens"] * growth_of_a_dollar_df["safeLPTokenPriceInETH"]
    )

    _add_BAL_and_AURA_minted_eth_value(growth_of_a_dollar_df)
    _add_extra_rewarders_incentive_eth_value(growth_of_a_dollar_df)
    growth_of_a_dollar_df["total_cumulative_incentive_tokens_current_eth_value"] = (
        growth_of_a_dollar_df["extra_rewarder_cumulative_incentive_eth_value"]
        + growth_of_a_dollar_df["main_rewarder_cumulative_incentive_eth_value"]
    )
    growth_of_a_dollar_df["growth_of_1_ETH"] = (
        growth_of_a_dollar_df["safe_value_of_lp_tokens"]
        + growth_of_a_dollar_df["total_cumulative_incentive_tokens_current_eth_value"]
    )
    return growth_of_a_dollar_df


def _add_BAL_and_AURA_minted_eth_value(growth_of_a_dollar_df: pd.DataFrame):
    starting_mintedBAL_rewardPerToken = growth_of_a_dollar_df["mintedBAL_rewardPerToken"].values[0]
    growth_of_a_dollar_df["BAL_minted_since_start_date"] = growth_of_a_dollar_df["starting_quantity_of_lp_tokens"] * (
        growth_of_a_dollar_df["mintedBAL_rewardPerToken"] - starting_mintedBAL_rewardPerToken
    )
    growth_of_a_dollar_df["AURA_minted_since_start_date"] = (
        growth_of_a_dollar_df["BAL_minted_since_start_date"] * growth_of_a_dollar_df["AURA_minted_for_one_BAL"]
    )
    growth_of_a_dollar_df["main_rewarder_cumulative_incentive_eth_value"] = (
        growth_of_a_dollar_df["BAL_minted_since_start_date"] * growth_of_a_dollar_df["BAL_to_ETH"]
    ) + (growth_of_a_dollar_df["AURA_minted_since_start_date"] * growth_of_a_dollar_df["AURA_to_ETH"])


def _add_extra_rewarders_incentive_eth_value(growth_of_a_dollar_df: pd.DataFrame):
    extra_reward_per_token_cols = [
        c for c in growth_of_a_dollar_df.columns if (("extraRewardPerToken" in c))
    ]  # format like AURA|| 0 extraRewardPerToken
    extra_reward_to_eth_cols = [f"{c.split('||')[0]}_to_ETH" for c in extra_reward_per_token_cols]
    growth_of_a_dollar_df["extra_rewarder_cumulative_incentive_eth_value"] = 0.0
    for extra_rewardPerToken_col, ETH_price_col in zip(extra_reward_per_token_cols, extra_reward_to_eth_cols):
        if "USDT" in ETH_price_col:
            ETH_price_col = "USDC_to_ETH"  # we don't have a pricer for USDT yet
        starting_rewardPerToken = growth_of_a_dollar_df[extra_rewardPerToken_col].values[0]
        extra_rewards_minted_since_start = growth_of_a_dollar_df["starting_quantity_of_lp_tokens"] * (
            growth_of_a_dollar_df[extra_rewardPerToken_col] - starting_rewardPerToken
        )
        growth_of_a_dollar_df["extra_rewarder_cumulative_incentive_eth_value"] += (
            extra_rewards_minted_since_start * growth_of_a_dollar_df[ETH_price_col]
        )


def _fetch_destination_growth_of_a_dollar_dfs(bal_rewarders_df: pd.DataFrame, blocks) -> pd.DataFrame:
    dfs = []
    for (
        destinationName,
        destination_vault_address,
        auraStaking_address,
        extraRewarders,
        extraRewardTokens,
        extraRewardersRewardTokenSymbol,
        extraRewardersRewardTokenDecimals,
    ) in zip(
        bal_rewarders_df["destinationName"],
        bal_rewarders_df["destination_vault_address"],
        bal_rewarders_df["auraStaking_address"],
        bal_rewarders_df["extraRewarders"],
        bal_rewarders_df["extraRewardTokens"],
        bal_rewarders_df["extraRewardersRewardTokenSymbol"],
        bal_rewarders_df["extraRewardersRewardTokenDecimals"],
    ):
        mainRewarder = auraStaking_address
        rewardTokens = [BAL, AURA, *extraRewardTokens]
        rewardTokenSymbols = ["BAL", "AURA", *extraRewardersRewardTokenSymbol]

        if destinationName == "Balancer rsETH-WETH Stable Pool":
            # this pool was deployed after time we stared to can ignore
            continue

        growth_of_a_dollar_calls = build_growth_of_a_dollar_calls(
            rewardTokens=rewardTokens,
            rewardTokenSymbols=rewardTokenSymbols,
            mainRewarder=mainRewarder,
            extraRewarders=extraRewarders,
            extraRewardersRewardTokenSymbols=extraRewardersRewardTokenSymbol,
            extraRewardersRewardTokenDecimals=extraRewardersRewardTokenDecimals,
            balancerAuraDestinationVault_address=destination_vault_address,
        )
        raw_growth_of_a_dollar_df = sync_safe_get_raw_state_by_block(growth_of_a_dollar_calls, blocks)
        raw_growth_of_a_dollar_df = raw_growth_of_a_dollar_df[
            raw_growth_of_a_dollar_df.index > GROWTH_OF_A_DOLLAR_START_DATE
        ].copy()
        growth_of_a_dollar_df = transform_growth_of_a_dollar_df_to_growth_of_1_ETH_with_no_entrance_costs(
            raw_growth_of_a_dollar_df
        )
        growth_of_a_dollar_df[f"{destinationName} Growth of a ETH"] = growth_of_a_dollar_df["growth_of_1_ETH"].astype(
            float
        )
        dfs.append(growth_of_a_dollar_df[f"{destinationName} Growth of a ETH"])
    growth_of_a_dollar_df = pd.concat(dfs, axis=1)
    return growth_of_a_dollar_df


def _fetch_balETH_nav_per_share_df(blocks):
    nav_per_share_call = Call(
        balETH_AUTOPOOL_ETH_ADDRESS,
        ["convertToAssets(uint256)(uint256)", int(1e18)],
        [("balETH", safe_normalize_with_bool_success)],
    )
    nav_per_share_df = sync_safe_get_raw_state_by_block([nav_per_share_call], blocks)
    return nav_per_share_df


def fetch_growth_of_a_dollar_df() -> pd.DataFrame:
    """Returns the growth of 1 ETH in each balancer destination since the start date vs balETH"""
    blocks = build_blocks_to_use()
    bal_rewarders_df = _fetch_required_addresses()
    nav_per_share_df = _fetch_balETH_nav_per_share_df(blocks)
    growth_of_a_dollar_df = _fetch_destination_growth_of_a_dollar_dfs(bal_rewarders_df, blocks)
    growth_of_a_dollar_df["balETH"] = nav_per_share_df["balETH"]
    return growth_of_a_dollar_df


@st.cache_data(ttl=12 * 3600)
def fetch_growth_of_a_dollar_figure():
    # Features to add
    # change start date
    # add sliders for the fees to charge on the other destiantions, reduce amount they start with
    # add toggle for (hold all incentive tokens), (periodicly sell all incentive tokens for ETH)
    # add dilution
    # add sliding factor for how much fees we collect on the autopool
    # add curve and maverick growth of a dollar

    growth_of_a_dollar_df = fetch_growth_of_a_dollar_df()
    fig = go.Figure()

    for col in growth_of_a_dollar_df.columns:
        if col != "balETH":
            fig.add_trace(
                go.Scatter(x=growth_of_a_dollar_df.index, y=growth_of_a_dollar_df[col], mode="lines", name=col)
            )

    fig.add_trace(
        go.Scatter(
            x=growth_of_a_dollar_df.index,
            y=growth_of_a_dollar_df["balETH"],
            mode="lines",
            name="balETH",
            line=dict(color="red", dash="dash"),
        )
    )

    fig.update_layout(title="Growth of 1 ETH Over Time", xaxis_title="Time", yaxis_title="ETH", height=800, width=1200)

    return fig
