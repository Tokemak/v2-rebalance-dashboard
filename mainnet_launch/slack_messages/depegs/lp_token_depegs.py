# not finished
from __future__ import annotations

import pandas as pd

from mainnet_launch.database.postgres_operations import _exec_sql_and_cache
from mainnet_launch.slack_messages.post_message import (
    post_message_with_table,
    SlackChannel,
    post_slack_message,
)

# Tune as you like
LP_DEPEG_THRESHOLD = 1.0  # percent


def _fetch_latest_lp_token_values() -> pd.DataFrame:
    """Return latest row per (destination_vault_address, chain_id) with pool and discount.

    percent_discount = 100 * (lp_token_safe_price - lp_token_spot_price) / lp_token_safe_price
    Positive => discount; Negative => premium
    """
    query = """
        SELECT DISTINCT ON (ds.destination_vault_address, ds.chain_id)
            ds.destination_vault_address,
            ds.chain_id,
            d.pool AS pool,                -- Pool name (from Destinations)
            d.symbol AS dest_symbol,       -- Optional: symbol for the LP token
            ds.lp_token_spot_price,
            ds.lp_token_safe_price,
            100.0 * (ds.lp_token_safe_price - ds.lp_token_spot_price) / ds.lp_token_safe_price
                AS percent_discount,       -- negative => premium, positive => discount
            ds.block,
            b.datetime
        FROM destination_states ds
        JOIN blocks b
          ON ds.block    = b.block
         AND ds.chain_id = b.chain_id
        JOIN destinations d
          ON ds.destination_vault_address = d.destination_vault_address
         AND ds.chain_id                  = d.chain_id
        WHERE ds.lp_token_spot_price IS NOT NULL
          AND ds.lp_token_safe_price IS NOT NULL
          AND ds.lp_token_safe_price <> 0
        ORDER BY ds.destination_vault_address, ds.chain_id, b.datetime DESC;
    """
    return _exec_sql_and_cache(query)


def post_lp_depeg_slack_message(slack_channel: SlackChannel) -> None:
    """Post a Slack table of LP tokens whose spot < safe by >= LP_DEPEG_THRESHOLD."""
    df = _fetch_latest_lp_token_values()
    if df.empty:
        post_slack_message("No LP token data found (or all LPs at/above safe).", slack_channel)
        return

    interesting_cols = [
        "dest_symbol",
        "destination_vault_address",
        "chain_id",
        "lp_token_spot_price",
        "lp_token_safe_price",
        "percent_discount",
        "datetime",
    ]

    # Only show discounts >= threshold
    lp_depeg_df = df  # df[df["percent_discount"] >= LP_DEPEG_THRESHOLD].copy()

    # Sort biggest discounts first for readability
    lp_depeg_df = lp_depeg_df.sort_values(["percent_discount", "datetime"], ascending=[False, False])

    post_message_with_table(
        slack_channel,
        f"LP tokens depegging (≥ {LP_DEPEG_THRESHOLD:.2f}%)",
        lp_depeg_df[interesting_cols],
        file_save_name="lp_depegs.csv",
    )


if __name__ == "__main__":
    # Example manual run
    post_lp_depeg_slack_message(SlackChannel.TESTING)

# Performance tip:
# CREATE INDEX IF NOT EXISTS ix_destination_states_addr_chain_block
#   ON destination_states (destination_vault_address, chain_id, block DESC);
