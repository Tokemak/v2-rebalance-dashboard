import pandas as pd

from mainnet_launch.data_fetching.internal.fetch_historical_prices import (
    TokemakPriceRequest,
    fetch_many_prices_from_internal_api,
)

from mainnet_launch.database.postgres_operations import _exec_sql_and_cache, insert_avoid_conflicts
from mainnet_launch.database.schema.full import IncentiveTokenPrices


def _get_needed_incentive_token_sales_prices() -> pd.DataFrame:

    # todo edit thit to include the block where the ClaimVaultRewards event was emitted as well
    query = """
    WITH full_details_of_needed_swapped_events AS (
        SELECT
            its.tx_hash,
            its.log_index,
            its.chain_id,
            its.sell_token_address,
            its.buy_token_address,
            tokens.decimals AS buy_token_decimals,
            b.datetime AS block_datetime
        FROM incentive_token_swapped AS its
        JOIN transactions AS t
            ON t.tx_hash = its.tx_hash
            AND t.chain_id = its.chain_id
        JOIN blocks AS b
            ON b.block = t.block
            AND b.chain_id = t.chain_id
        JOIN tokens
            on tokens.token_address = its.buy_token_address
            and tokens.chain_id = its.chain_id
    )
    SELECT c.*
    FROM full_details_of_needed_swapped_events c
    WHERE NOT EXISTS (
        SELECT 1
        FROM incentive_token_sale_prices p
        WHERE p.tx_hash = c.tx_hash
            AND p.log_index = c.log_index
    );
    """
    needed_incentive_token_sales_prices_df = _exec_sql_and_cache(query)
    return needed_incentive_token_sales_prices_df


def _build_tokemak_price_requests(needed_prices_df: pd.DataFrame) -> list[TokemakPriceRequest]:
    """
    Convert a dataframe of needed incentive token sale prices into a list of TokemakPriceRequest objects.
    """
    requests: list[TokemakPriceRequest] = []
    for _, row in needed_prices_df.iterrows():

        ts = pd.Timestamp(row.block_datetime)

        requests.append(
            TokemakPriceRequest(
                chain_id=row.chain_id,
                token_to_price=row.sell_token_address,
                denominate_in=row.buy_token_address,
                denominate_in_decimals=row.buy_token_decimals,
                timestamp=int(ts.timestamp()),
            )
        )

    return requests


def ensure_incentive_token_prices_are_current():
    needed_incentive_token_sales_prices_df = _get_needed_incentive_token_sales_prices()
    if needed_incentive_token_sales_prices_df.empty:
        # early exit since we already have all of the prices we need
        return
    price_requests = _build_tokemak_price_requests(needed_incentive_token_sales_prices_df)
    price_df = fetch_many_prices_from_internal_api(price_requests, 200 // 3, 10)

    full_df = pd.concat([needed_incentive_token_sales_prices_df, price_df], axis=1)

    new_incentive_token_prices = full_df.apply(
        lambda row: IncentiveTokenPrices(
            tx_hash=row.tx_hash,
            log_index=row.log_index,
            third_party_price=row.price,
        ),
        axis=1,
    ).tolist()

    insert_avoid_conflicts(new_incentive_token_prices, IncentiveTokenPrices)


if __name__ == "__main__":
    ensure_incentive_token_prices_are_current()
