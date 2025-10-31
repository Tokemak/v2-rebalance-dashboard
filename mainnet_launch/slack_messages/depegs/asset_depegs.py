"""
Post a Slack message for the currently depegging assets and our exposure to them
"""

import pandas as pd
import numpy as np

from mainnet_launch.constants import ALL_CHAINS
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache

from mainnet_launch.slack_messages.post_message import post_message_with_table, SlackChannel, post_slack_message
from mainnet_launch.database.views import get_token_details_dict

STABLE_COIN_DEPEG_OR_PREMIUM_PERCENT_THRESHOLD = 0.05
ETH_DEPEG_OR_PREMIUM_PERCENT_THRESHOLD = 0.1

# Prices and exposure within the 2 days eg directionally correct but not exact
# TODO consider making them line up exactly


def _fetch_latest_token_prices() -> pd.DataFrame:
    two_days_ago = pd.Timestamp.now() - pd.Timedelta(days=1)
    query = f"""
    SELECT DISTINCT ON (tv.token_address, tv.denominated_in, tv.chain_id)
            tv.token_address,
            tv.denominated_in,
            tv.chain_id,
            t.symbol,
            tv.safe_price,
            tv.backing,
            100 * (tv.backing - tv.safe_price) / tv.backing as percent_discount, -- negative means premimum, positive means discount
            tv.block,
            b.datetime as price_datetime
        FROM token_values tv
        JOIN tokens t
        ON tv.token_address = t.token_address
        AND tv.chain_id     = t.chain_id
        
        JOIN blocks b
        ON tv.block = b.block
        AND tv.chain_id = b.chain_id

        WHERE tv.safe_price IS NOT NULL
        AND tv.backing    IS NOT NULL
        AND safe_price < 5 -- compares apples to apples, eg pxETH in ETH terms not USDC terms

        -- AND  b.datetime >= '{two_days_ago}'

        ORDER BY tv.token_address, tv.denominated_in, tv.chain_id, b.datetime DESC;

    """

    df = _exec_sql_and_cache(query)
    return df


def _fetch_latest_asset_exposure() -> pd.DataFrame:
    two_days_ago = pd.Timestamp.now() - pd.Timedelta(days=1)
    query = f"""
    SELECT DISTINCT ON (ae.chain_id, ae.reference_asset, ae.token_address)
        ae.chain_id,
        ae.reference_asset,
        ae.token_address,
        b.datetime as exposure_datetime,
        ae.quantity

        FROM asset_exposure ae
        JOIN tokens t
        ON t.token_address = ae.token_address
        AND t.chain_id      = ae.chain_id
        JOIN tokens tref
        ON tref.token_address = ae.reference_asset
        AND tref.chain_id      = ae.chain_id
        JOIN blocks b
        ON ae.block = b.block
        AND ae.chain_id = b.chain_id

        WHERE
        b.datetime >= '{two_days_ago}'

        ORDER BY
        ae.chain_id, ae.reference_asset, ae.token_address,
        ae.block DESC, ae.quote_batch DESC;
    """
    df = _exec_sql_and_cache(query)
    return df


def fetch_recent_prices_and_exposure() -> pd.DataFrame:
    df = _fetch_latest_token_prices()
    recent_exposure_df = _fetch_latest_asset_exposure()
    full_df = pd.merge(
        df,
        recent_exposure_df,
        left_on=["token_address", "denominated_in", "chain_id"],
        right_on=["token_address", "reference_asset", "chain_id"],
        how="left",
    )
    token_to_decimals, token_to_symbol = get_token_details_dict()
    full_df["token_symbol"] = full_df["token_address"].map(token_to_symbol)
    full_df["reference_symbol"] = full_df["denominated_in"].map(token_to_symbol)

    full_df["value_at_safe"] = full_df["safe_price"] * full_df["quantity"]
    full_df["value_at_backing"] = full_df["backing"] * full_df["quantity"]
    full_df["chain_name"] = full_df["chain_id"].map({c.chain_id: c.name for c in ALL_CHAINS})
    interesting_cols = [
        "token_symbol",
        "reference_symbol",
        "chain_id",
        "safe_price",
        "backing",
        "percent_discount",
        "quantity",
        "exposure_datetime",
        "price_datetime",
        "value_at_safe",
        "value_at_backing",
        "chain_name",
    ]
    return full_df[interesting_cols]


def summarize_discounts_by_reference(full_df: pd.DataFrame) -> pd.DataFrame:
    """
    By reference asset (eg USDC, WETH), chain) summarize total exposure at safe price vs backing price,
    """
    some_exposure_df = full_df[(full_df["quantity"] > 0)].copy()
    agg = (
        some_exposure_df.groupby(["chain_name", "reference_symbol"], dropna=False)[
            ["value_at_safe", "value_at_backing"]
        ]
        .sum()
        .reset_index()
        .rename(
            columns={
                "value_at_safe": "total_value_at_safe_price",
                "value_at_backing": "total_value_at_backing",
            }
        )
    )

    agg["overall_percent_discount"] = (
        100.0 * (agg["total_value_at_backing"] - agg["total_value_at_safe_price"]) / agg["total_value_at_backing"]
    )
    agg = agg.sort_values("overall_percent_discount", ascending=False, ignore_index=True)

    agg["overall_percent_discount"] = agg["overall_percent_discount"].map("{:.2f}%".format).astype(str)
    agg["total_value_at_safe_price"] = agg["total_value_at_safe_price"].map("{:.2f}".format).astype(str)
    agg["total_value_at_backing"] = agg["total_value_at_backing"].map("{:.2f}".format).astype(str)
    return agg


def post_asset_depeg_slack_message(slack_channel: SlackChannel):
    df = fetch_recent_prices_and_exposure()
    price_return_summary_df = summarize_discounts_by_reference(df)
    post_message_with_table(
        slack_channel,
        "Summary of depegging exposure by reference asset",
        price_return_summary_df,
        file_save_name="depeg_summary_by_reference.csv",
    )

    df = df[df["quantity"] > 0].copy()

    df["non_trivial_discount"] = np.where(
        (df["reference_symbol"] == "WETH") & (df["percent_discount"].abs() >= ETH_DEPEG_OR_PREMIUM_PERCENT_THRESHOLD),
        True,
        np.where(df["percent_discount"].abs() >= STABLE_COIN_DEPEG_OR_PREMIUM_PERCENT_THRESHOLD, True, False),
    )

    non_trivial_depeg_df = df[df["non_trivial_discount"]].copy()
    if non_trivial_depeg_df.empty:
        post_slack_message("No depegging assets found with non-trivial discounts.", slack_channel)
        return
    else:
        post_message_with_table(
            slack_channel,
            "All depegging assets with non-trivial discounts or premiums",
            non_trivial_depeg_df,
            file_save_name="non_trivial_depegs_and_exposure.csv",
        )


if __name__ == "__main__":
    post_asset_depeg_slack_message(SlackChannel.TESTING)


# plasma sUSDe USDT0 chain is showing prices from oct 22, not sure hwy
# ,sUSDe,USDT0,9745,1.199316,1.203265411509638,0.32822446917034703,2187168.9378375076,2025-10-31 01:56:58+00:00,2025-10-22 21:41:31+00:00,2623106.701851528,2631744.7320281463,plasma,True
