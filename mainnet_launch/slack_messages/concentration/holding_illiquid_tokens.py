import pandas as pd

from mainnet_launch.pages.risk_metrics.render_exit_liquidity_batch import (
    _load_quote_batch_options_from_db,
    _fetch_asset_allocation_from_db,
    _load_full_quote_batch_df,
    identify_suspect_exit_liquidity_quotes,
)
from mainnet_launch.database.postgres_operations import (
    get_full_table_as_df,
    _exec_sql_and_cache,
)
from mainnet_launch.constants.chains import ALL_CHAINS
from mainnet_launch.slack_messages.post_message import post_message_with_table, SlackChannel, post_slack_message


def fetch_latest_50th_percentile_quote_batch_data():
    """Fetch the batch of quotes from our api and"""
    options = _load_quote_batch_options_from_db()
    latest_50th_percentile_option = [o for o in options if o["percent_exclude_threshold"] == 50][0]
    latest_batch_number = latest_50th_percentile_option["quote_batch"]
    asset_exposure_df = _fetch_asset_allocation_from_db(latest_batch_number)
    swap_quotes_df, median_reference_prices = _load_full_quote_batch_df(latest_batch_number)

    median_df = (
        swap_quotes_df.groupby(["api_name", "buy_token_symbol", "sell_token_symbol", "scaled_amount_in", "chain_id"])[
            "slippage_bps"
        ]
        .mean()
        .reset_index()
    )
    median_df["bps_threshold"] = median_df["buy_token_symbol"].apply(lambda symbol: 50 if symbol == "WETH" else 25)

    above_threshold_df = median_df[median_df["slippage_bps"] > median_df["bps_threshold"]].copy()
    chain_id_to_name = {c.chain_id: c.name for c in ALL_CHAINS}
    above_threshold_df["chain_name"] = above_threshold_df["chain_id"].map(chain_id_to_name)
    asset_exposure_df = asset_exposure_df[["token_symbol", "reference_symbol", "quantity", "chain_id"]]
    return above_threshold_df, asset_exposure_df


def determine_maybe_over_exposed_assets(suspect_quotes_df: pd.DataFrame, asset_exposure_df: pd.DataFrame):
    df = pd.merge(
        suspect_quotes_df,
        asset_exposure_df,
        left_on=["chain_id", "sell_token_symbol"],
        right_on=["chain_id", "token_symbol"],
    )
    df["percent_sold_that_breaks_slippage_threshold"] = ((df["scaled_amount_in"] / df["quantity"]) * 100).round(2)
    df = df[df["quantity"] > df["scaled_amount_in"]]  # only look at currently over exposed
    only_tokemak_df = df[df["api_name"] == "tokemak"].copy()

    maybe_over_exposed_df = (
        only_tokemak_df.groupby(["api_name", "chain_name", "sell_token_symbol", "buy_token_symbol"])
        .agg(
            {
                "percent_sold_that_breaks_slippage_threshold": "min",
                "quantity": "first",
            }
        )
        .reset_index()
    )
    maybe_over_exposed_df["minimal_safe_sellable_quantity"] = maybe_over_exposed_df["quantity"] * (
        maybe_over_exposed_df["percent_sold_that_breaks_slippage_threshold"] / 100
    )

    maybe_over_exposed_df = maybe_over_exposed_df.sort_values(
        by="percent_sold_that_breaks_slippage_threshold", ascending=True
    )
    maybe_over_exposed_df["quantity"] = maybe_over_exposed_df["quantity"].map(lambda x: f"{x:,.2f}")
    maybe_over_exposed_df["minimal_safe_sellable_quantity"] = maybe_over_exposed_df[
        "minimal_safe_sellable_quantity"
    ].map(lambda x: f"{x:,.2f}")
    maybe_over_exposed_df["percent_sold_that_breaks_slippage_threshold"] = maybe_over_exposed_df[
        "percent_sold_that_breaks_slippage_threshold"
    ].map(lambda x: f"{x:.2f}%")

    maybe_over_exposed_df.rename(
        columns={
            "chain_name": "Chain",
            "sell_token_symbol": "Sell Token",
            "buy_token_symbol": "Buy Token",
            "percent_sold_that_breaks_slippage_threshold": "% Safely Sellable",
            "quantity": "Our Exposure",
            "minimal_safe_sellable_quantity": "Safe Sellable Quantity",
        },
        inplace=True,
    )

    return maybe_over_exposed_df[
        ["Chain", "Sell Token", "Buy Token", "Our Exposure", "% Safely Sellable", "Safe Sellable Quantity"]
    ]


def post_illiquid_token_holding_analysis(slack_channel: SlackChannel):
    """

    The way to interpert this is

    Tn the lastest batch of swap quotes from tokemak
    for those asset pairs, we have to sell X% of our total exposure in order to break the slippage threshold
    and this is how much we have in total exposure.
    so the safe quantity to sell without exceeding the slippage threshold is minimal_safe_sellable_quantity

    eg, according to our api
    on base, if we sell 5.86% of our GHO holdings for USDC we exceed the 25 bps slippage threshold, so the safe quanityt to sell right now is 200k
    """

    suspect_quotes_df, asset_exposure_df = fetch_latest_50th_percentile_quote_batch_data()
    maybe_over_exposed_df = determine_maybe_over_exposed_assets(suspect_quotes_df, asset_exposure_df)

    if maybe_over_exposed_df.empty:
        post_slack_message(
            slack_channel,
            "No illiquid token holdings detected based on our latest exit liquidity analysis. All good!",
        )
    else:
        post_message_with_table(
            slack_channel,
            "Exposure to illiquid tokens Illiquid = stable coin quote slippage >25bps or ETH asset >50bps",
            maybe_over_exposed_df,
            file_save_name="illiquid_token_exposure.csv",
        )


if __name__ == "__main__":
    post_illiquid_token_holding_analysis(SlackChannel.TESTING)
