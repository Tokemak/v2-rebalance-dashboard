"""These are assumed to be ran after the production database is updated"""

import pandas as pd

from mainnet_launch.database.postgres_operations import _exec_sql_and_cache
from mainnet_launch.slack_messages.post_message import SlackChannel, post_message_with_table


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


def post_autopools_without_generated_plans():
    df = _get_autopools_without_a_plan_in_the_last_n_days(2)
    post_message_with_table(
        channel=SlackChannel.TESTING,
        initial_comment="Autopools without a rebalance plan generated in the last 2 days",
        df=df,
        file_save_name="Autopools without recent plans.csv",
    )


if __name__ == "__main__":
    post_autopools_without_generated_plans()
