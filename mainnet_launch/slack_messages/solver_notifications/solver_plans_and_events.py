"""These are assumed to be ran after the production database is updated"""

import pandas as pd
from blockkit import Message, Section, Button, Confirm, MarkdownText

from mainnet_launch.database.postgres_operations import _exec_sql_and_cache

# something about
# diff between last proposed rebalance and last rebalance event


def pick_emoji(days: float) -> str:
    if days < 1:
        return "🟢"  # green
    elif 1 <= days < 2:
        return "🟡"  # yellow
    elif 2 <= days < 3:
        return "🟠"  # orange
    else:
        return "🔴"  # red


def _get_autopools_without_a_plan_in_the_last_n_days(n_days: int):
    query = """
        SELECT 
            autopools.symbol as autopool_symbol,
            MAX(datetime_generated) as plan_generated_time
        FROM 
            rebalance_plans
        JOIN 
            autopools
        ON 
            autopools.autopool_vault_address = rebalance_plans.autopool_vault_address
        GROUP BY 
            autopools.symbol
    """

    df = _exec_sql_and_cache(query)
    df["time_since_plan_generated"] = pd.Timestamp.now(tz="UTC").floor("min") - df["plan_generated_time"].dt.floor(
        "min"
    )
    df = (
        df[df["time_since_plan_generated"] > pd.Timedelta(days=n_days)]
        .reset_index()
        .sort_values("time_since_plan_generated")
    )
    return df[["autopool_symbol", "time_since_plan_generated"]]


def _get_autopools_without_a_rebalance_event_in_the_last_n_days(n_days: int):
    query = """
        SELECT
        a.symbol AS autopool_symbol,
        MAX(b.datetime) AS last_rebalance_time
        FROM autopools a
        LEFT JOIN rebalance_events re
        ON re.autopool_vault_address = a.autopool_vault_address
        AND re.chain_id = a.chain_id
        LEFT JOIN transactions tx
        ON tx.tx_hash = re.tx_hash
        LEFT JOIN blocks b
        ON b.block = tx.block
        AND b.chain_id = tx.chain_id
        GROUP BY a.autopool_vault_address, a.chain_id, a.symbol
    """

    df = _exec_sql_and_cache(query)
    df["time_since_rebalance_event"] = pd.Timestamp.now(tz="UTC").floor("min") - df["last_rebalance_time"].dt.floor(
        "min"
    )
    df = (
        df[df["time_since_rebalance_event"] > pd.Timedelta(days=n_days)]
        .reset_index()
        .sort_values("time_since_rebalance_event")
    )
    return df[["autopool_symbol", "time_since_rebalance_event"]]


def _df_to_blockkit_markdown_table(df: pd.DataFrame) -> str:
    """Converts a pandas DataFrame to a markdown table string."""
    df_in_markdown = df.to_markdown(index=False)
    message = Message(
        blocks=[
            Section(text=MarkdownText(text=f"```{df_in_markdown}```")),
        ]
    )


def _get_autopools_without_a_proposed_rebalance_in_the_last_n_days(n_days: int):

    query = """
        SELECT 
            autopools.symbol as autopool_symbol,
            MAX(datetime_generated) as plan_generated_time
        FROM 
            rebalance_plans
        JOIN 
            autopools
        ON 
            autopools.autopool_vault_address = rebalance_plans.autopool_vault_address

        WHERE
            rebalance_plans.move_name is NOT NULL
        GROUP BY 
            autopools.symbol
    """
    df = _exec_sql_and_cache(query)
    df["time_since_plan_generated"] = pd.Timestamp.now(tz="UTC").floor("min") - df["plan_generated_time"].dt.floor(
        "min"
    )
    df = (
        df[df["time_since_plan_generated"] > pd.Timedelta(days=n_days)]
        .reset_index()
        .sort_values("time_since_plan_generated")
    )

    return df[["autopool_symbol", "time_since_plan_generated"]]


if __name__ == "__main__":
    df = _get_autopools_without_a_proposed_rebalance_in_the_last_n_days()
    df_in_markdown = df.to_markdown(index=False)

    message = Message(
        blocks=[
            Section(text=MarkdownText(text=f"```{df_in_markdown}```")),
        ]
    )

    print(message.build())

    # print(_get_autopools_without_a_rebalance_event_in_the_last_n_days(2))

    # print(_get_autopools_without_a_proposed_rebalance_in_the_last_n_days(2))

    # not sure if I want this
    # add later if at all
    # no plans and no events good enough to look close

    # not events -> maybe prob
    # no plans -> for sure prob
