"""
Look at all tokens sent to the incentive liquidation row in the last 30 days, 

If any of those have non trvial balances still in the liquidation row, report them as "not recently sold tokens".
and report those via slack.

"""

import pandas as pd

from mainnet_launch.database.postgres_operations import (
    _exec_sql_and_cache,
)
from mainnet_launch.database.schema.full import *
from mainnet_launch.slack_messages.post_message import post_slack_message, post_message_with_table, SlackChannel
from mainnet_launch.slack_messages.constants import CircleEmoji
from mainnet_launch.constants import *

from mainnet_launch.database.views import get_token_details_dict

from datetime import datetime, timedelta

from multicall import Call
from mainnet_launch.data_fetching.get_state_by_block import (
    safe_normalize_6_with_bool_success,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
    build_blocks_to_use,
)


def _fetch_addresses_of_tokens_sent_to_liquidation_row_in_prior_month() -> pd.DataFrame:
    month_ago = datetime.utcnow() - timedelta(days=30)
    query = f"""
        SELECT
            incentive_token_balance_updated.liquidation_row, 
            incentive_token_balance_updated.chain_id,
            ARRAY_AGG(DISTINCT incentive_token_balance_updated.token_address) AS token_addresses
        FROM 
            incentive_token_balance_updated
        JOIN
            transactions 
            ON incentive_token_balance_updated.tx_hash = transactions.tx_hash 
            AND incentive_token_balance_updated.chain_id = transactions.chain_id
        JOIN
            blocks ON transactions.block = blocks.block AND transactions.chain_id = blocks.chain_id

        WHERE 
            blocks.datetime >= '{month_ago.isoformat()}'

        GROUP BY 
            liquidation_row,
            incentive_token_balance_updated.chain_id
    """
    df = _exec_sql_and_cache(query)
    return df.explode("token_addresses", ignore_index=True)


def _fetch_incentive_tokens_sold_in_prior_week():
    week_ago = datetime.utcnow() - timedelta(days=7)
    query = f"""
        SELECT
            incentive_token_swapped.liquidation_row, 
            incentive_token_swapped.chain_id,
            ARRAY_AGG(DISTINCT incentive_token_swapped.sell_token_address) AS token_addresses
        FROM 
            incentive_token_swapped
        JOIN
            transactions 
            ON incentive_token_swapped.tx_hash = transactions.tx_hash 
            AND incentive_token_swapped.chain_id = transactions.chain_id
        JOIN
            blocks ON transactions.block = blocks.block AND transactions.chain_id = blocks.chain_id

        WHERE 
            blocks.datetime >= '{week_ago.isoformat()}'

        GROUP BY 
            liquidation_row,
            incentive_token_swapped.chain_id
    """
    df = _exec_sql_and_cache(query)
    return df.explode("token_addresses", ignore_index=True)


def _add_current_liqudation_row_balances(df: pd.DataFrame):
    all_yesterday_balances = {}
    all_today_balances = {}
    for chain in ALL_CHAINS:
        sub_df = df[df["chain_id"] == chain.chain_id]
        if sub_df.empty:
            continue

        calls = sub_df.apply(
            lambda row: Call(
                row["token_addresses"],
                ["balanceOf(address)(uint256)", row["liquidation_row"]],
                [
                    (
                        (row["liquidation_row"], row["token_addresses"]),
                        (
                            safe_normalize_with_bool_success
                            if row["decimals"] == 18
                            else safe_normalize_6_with_bool_success
                        ),
                    )
                ],
            ),
            axis=1,
        ).tolist()
        blocks = build_blocks_to_use(chain)
        today_block, yesterday_block = blocks[-1], blocks[-2]
        today_balances = get_state_by_one_block(calls, today_block, chain)
        all_today_balances.update(today_balances)

        yesterday_balances = get_state_by_one_block(calls, yesterday_block, chain)
        all_yesterday_balances.update(yesterday_balances)

    df["today_balance"] = df.apply(
        lambda row: all_today_balances[row["liquidation_row"], row["token_addresses"]], axis=1
    )
    df["yesterday_balance"] = df.apply(
        lambda row: all_yesterday_balances[row["liquidation_row"], row["token_addresses"]], axis=1
    )


def post_unsold_incentive_tokens(slack_channel: SlackChannel):
    """
    Post a table of incentive tokens sent to liquidation rows in the past month that still have (near) non-zero balance.

    Depending on timing this may have some false positives if the token was only very recently sent to the liquidation row.

    eg sent to liqudation row, but not yet sold. perhaps on mainnet where we doing may batch the sales during low gas cost periods.
    """
    expected_tokens_to_be_sold = _fetch_addresses_of_tokens_sent_to_liquidation_row_in_prior_month()
    token_to_decimals, token_to_symbol = get_token_details_dict()
    expected_tokens_to_be_sold["decimals"] = expected_tokens_to_be_sold["token_addresses"].map(token_to_decimals)
    expected_tokens_to_be_sold["symbol"] = expected_tokens_to_be_sold["token_addresses"].map(token_to_symbol)
    expected_tokens_to_be_sold["chain_name"] = expected_tokens_to_be_sold["chain_id"].map(
        {chain.chain_id: chain.name for chain in ALL_CHAINS}
    )
    _add_current_liqudation_row_balances(expected_tokens_to_be_sold)
    pass

    # required part of a true positive, 1e-6 is $.10 for BTC at 100k, can safely ignore everything less than that
    balances_to_watch_df = expected_tokens_to_be_sold[expected_tokens_to_be_sold["yesterday_balance"] > 1e-6].copy()
    balances_to_watch_df["warning"] = balances_to_watch_df.apply(
        lambda row: (
            CircleEmoji.RED.value if row["today_balance"] > row["yesterday_balance"] else CircleEmoji.YELLOW.value
        ),
        axis=1,
    )

    if not balances_to_watch_df.empty:
        debank_urls = [
            f"https://debank.com/profile/{a}" for a in balances_to_watch_df["liquidation_row"].unique().tolist()
        ]
        post_message_with_table(
            slack_channel,
            initial_comment="Incentive Tokens Balances in > 0 yesterday \n" + str(debank_urls),
            df=balances_to_watch_df[
                ["warning", "liquidation_row", "chain_name", "symbol", "today_balance", "yesterday_balance"]
            ],
            file_save_name="liquidation_row_balances_to_check.csv",
            show_index=False,
        )


def _determine_what_should_be_sold_but_was_not(
    actually_sold: pd.DataFrame, expected_tokens_to_be_sold: pd.DataFrame
) -> pd.DataFrame:
    print("Expected to be sold:")
    print(expected_tokens_to_be_sold.shape)
    print("Actually sold:")
    print(actually_sold.shape)

    chain_name = {chain.chain_id: chain.name for chain in ALL_CHAINS}
    expected_tokens_to_be_sold["chain_name"] = expected_tokens_to_be_sold["chain_id"].map(chain_name)
    actually_sold["chain_name"] = actually_sold["chain_id"].map(chain_name)
    # actually_sold = actually_sold[actually_sold['chain_id'] != 1].reset_index(drop=True)
    diff = expected_tokens_to_be_sold.merge(actually_sold, how="outer", indicator=True)
    expected_but_not_actually_sold = diff[diff["_merge"] == "left_only"].drop(columns="_merge")
    return expected_but_not_actually_sold


def post_expected_but_not_recently_sold_tokens(slack_channel: SlackChannel):
    # not I don't think this will catch everthing but it may be interesting as a reference
    return
    actually_sold = _fetch_incentive_tokens_sold_in_prior_week()
    expected_tokens_to_be_sold = _fetch_addresses_of_tokens_sent_to_liquidation_row_in_prior_month()
    token_to_decimals, token_to_symbol = get_token_details_dict()
    expected_tokens_to_be_sold["decimals"] = expected_tokens_to_be_sold["token_addresses"].map(token_to_decimals)
    expected_tokens_to_be_sold["symbol"] = expected_tokens_to_be_sold["token_addresses"].map(token_to_symbol)
    actually_sold["decimals"] = actually_sold["token_addresses"].map(token_to_decimals)
    actually_sold["symbol"] = actually_sold["token_addresses"].map(token_to_symbol)

    expected_but_not_actually_sold = _determine_what_should_be_sold_but_was_not(
        actually_sold, expected_tokens_to_be_sold
    )
    _add_current_liqudation_row_balances(expected_but_not_actually_sold)
    if expected_but_not_actually_sold.empty:
        print("All incentive tokens sent to liquidation rows were recently sold.")
        return

    post_message_with_table(
        slack_channel,
        initial_comment="Incentive tokens sent to liquidation rows in the past month but not recently sold:",
        df=expected_but_not_actually_sold[["liquidation_row", "chain_name", "symbol", "balance"]],
        file_save_name="expected_but_not_recently_sold_incentive_tokens.csv",
    )


if __name__ == "__main__":
    post_unsold_incentive_tokens(SlackChannel.TESTING)
