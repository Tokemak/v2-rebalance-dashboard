"""this makes sure that we get a 3rd party price (at that moment) for each incentive token, whenever we either sell or claim incentive tokens"""

import pandas as pd

from mainnet_launch.data_fetching.internal.fetch_historical_prices import (
    TokemakPriceRequest,
    fetch_many_prices_from_internal_api,
)

from mainnet_launch.database.postgres_operations import _exec_sql_and_cache, insert_avoid_conflicts
from mainnet_launch.database.schema.full import IncentiveTokenPrices


def _get_needed_incentive_token_sales_prices_from_claim_vault_rewards() -> pd.DataFrame:
    query = """
     WITH full_details_of_needed_claim_vault_rewards AS (
            SELECT

            claim_vault_rewards.tx_hash,
            claim_vault_rewards.destination_vault_address,
            claim_vault_rewards.log_index,
            claim_vault_rewards.chain_id,
            claim_vault_rewards.token_address as sell_token_address,
            sell_tok.decimals AS sell_token_decimals,
            b.datetime AS block_datetime,
            destinations.denominated_in as buy_token_address,
            destinations.underlying_name,
            buy_tok.decimals AS buy_token_decimals

            FROM claim_vault_rewards

            JOIN transactions AS t
                ON t.tx_hash = claim_vault_rewards.tx_hash
                AND t.chain_id = claim_vault_rewards.chain_id
            JOIN blocks AS b
                ON b.block = t.block
                AND b.chain_id = t.chain_id
            JOIN tokens as sell_tok
                ON sell_tok.token_address = claim_vault_rewards.token_address
                AND sell_tok.chain_id = claim_vault_rewards.chain_id
            JOIN destinations
                ON destinations.destination_vault_address = claim_vault_rewards.destination_vault_address
                AND destinations.chain_id = claim_vault_rewards.chain_id  
            JOIN tokens AS buy_tok
                ON buy_tok.token_address = destinations.denominated_in
                AND buy_tok.chain_id  = claim_vault_rewards.chain_id
        )
        select full_details_of_needed_claim_vault_rewards.* from 
        full_details_of_needed_claim_vault_rewards WHERE NOT EXISTS (
            SELECT 1 from incentive_token_prices p 
            WHERE p.tx_hash = full_details_of_needed_claim_vault_rewards.tx_hash
            AND p.log_index = full_details_of_needed_claim_vault_rewards.log_index
            AND p.token_address = full_details_of_needed_claim_vault_rewards.token_address
        );

    """
    claim_vault_rewards_incentive_tokens_to_price_df = _exec_sql_and_cache(query)
    return claim_vault_rewards_incentive_tokens_to_price_df


def _get_needed_incentive_token_sales_prices_from_incentive_tokens_swapped() -> pd.DataFrame:
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
            ON tokens.token_address = its.buy_token_address
            AND tokens.chain_id = its.chain_id
    )
    SELECT full_details_of_needed_swapped_events.*
    
    FROM full_details_of_needed_swapped_events 
    WHERE NOT EXISTS (
        SELECT 1
        FROM incentive_token_sale_prices p
        WHERE p.tx_hash = full_details_of_needed_swapped_events.tx_hash
            AND p.log_index = full_details_of_needed_swapped_events.log_index
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


def _update_incentive_token_prices_for_incentive_token_sales():
    needed_incentive_token_sales_prices_df = _get_needed_incentive_token_sales_prices_from_incentive_tokens_swapped()
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
            chain_id=row.chain_id,
            token_address=row.sell_token_address,
            token_price_denomiated_in=row.buy_token_address,
        ),
        axis=1,
    ).tolist()

    insert_avoid_conflicts(new_incentive_token_prices, IncentiveTokenPrices)


def _update_incentive_token_prices_for_claim_vault_rewards():
    claim_vault_rewards_prices_df = _get_needed_incentive_token_sales_prices_from_claim_vault_rewards()
    if claim_vault_rewards_prices_df.empty:
        # early exit since we already have all of the prices we need
        return

    price_requests = _build_tokemak_price_requests(claim_vault_rewards_prices_df)
    price_df = fetch_many_prices_from_internal_api(price_requests, 200 // 3, 10)

    full_df = pd.concat([claim_vault_rewards_prices_df, price_df], axis=1)

    new_incentive_token_prices = full_df.apply(
        lambda row: IncentiveTokenPrices(
            tx_hash=row.tx_hash,
            log_index=row.log_index,
            third_party_price=row.price,
            chain_id=row.chain_id,
            token_address=row.sell_token_address,
            token_price_denomiated_in=row.buy_token_address,
        ),
        axis=1,
    ).tolist()

    insert_avoid_conflicts(new_incentive_token_prices, IncentiveTokenPrices)


def ensure_incentive_token_prices_are_current():
    _update_incentive_token_prices_for_incentive_token_sales()
    _update_incentive_token_prices_for_claim_vault_rewards()


if __name__ == "__main__":
    ensure_incentive_token_prices_are_current()
