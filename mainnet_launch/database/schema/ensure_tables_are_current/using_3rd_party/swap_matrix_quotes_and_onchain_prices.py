import time
import random
import pandas as pd
from multicall import Call

from aiolimiter import AsyncLimiter
from tqdm import tqdm
from mainnet_launch.constants import *
from concurrent.futures import ThreadPoolExecutor

from mainnet_launch.database.postgres_operations import (
    _exec_sql_and_cache,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    get_block_by_timestamp_defi_llama,
    get_block_by_timestamp_alchemy,
)

from mainnet_launch.data_fetching.internal.fetch_quotes import (
    fetch_many_swap_quotes_from_internal_api,
    fetch_single_swap_quote_from_internal_api,
    TokemakQuoteRequest,
    THIRD_PARTY_SUCCESS_KEY,
)


from mainnet_launch.data_fetching.get_state_by_block import (
    safe_normalize_6_with_bool_success,
    get_state_by_one_block,
    safe_normalize_with_bool_success,
)


def get_autopool_possible_assets(autopool: AutopoolConstants):
    query = f"""
    
        with valid_destinations as (
    select destination_vault_address from autopool_destinations

    WHERE autopool_destinations.autopool_vault_address = '{autopool.autopool_eth_addr}'
    ),

    this_autopool_asset_tokens as (

    select distinct token_address from destination_tokens 

    WHERE destination_tokens.destination_vault_address in (select destination_vault_address from valid_destinations)
    )

    select chain_id, token_address, symbol, name, decimals from tokens where tokens.token_address in (select token_address from this_autopool_asset_tokens)"""

    df = _exec_sql_and_cache(query)

    if autopool == AUTO_ETH:
        other_df = _exec_sql_and_cache(
            f"""
    
        with valid_destinations as (
    select destination_vault_address from autopool_destinations

    WHERE autopool_destinations.autopool_vault_address = '{AUTO_LRT.autopool_eth_addr}'
    ),

    this_autopool_asset_tokens as (

    select distinct token_address from destination_tokens 

    WHERE destination_tokens.destination_vault_address in (select destination_vault_address from valid_destinations)
    )

    select chain_id, token_address, symbol, name, decimals from tokens where tokens.token_address in (select token_address from this_autopool_asset_tokens)"""
        )
        df = pd.concat([df, other_df]).drop_duplicates().reset_index(drop=True)
    return df


def build_fetch_on_chain_spot_prices_function(autopool: AutopoolConstants):

    def _fetch_on_chain_spot_prices(tokemak_quote_response: dict) -> dict:
        i = 0
        while i < 3:
            try:
                unix_timestamp = int(pd.to_datetime(tokemak_quote_response["datetime_received"], utc=True).timestamp())
                block = get_block_by_timestamp_alchemy(unix_timestamp, autopool.chain, closest="before")
                found_timestamp = autopool.chain.client.eth.get_block(block)

                norm_function = (
                    safe_normalize_6_with_bool_success
                    if autopool.base_asset_decimals == 6
                    else safe_normalize_with_bool_success
                )

                buy_token_price_call = Call(
                    ROOT_PRICE_ORACLE(autopool.chain),
                    [
                        "getPriceInQuote(address,address)(uint256)",
                        tokemak_quote_response["buyToken"],
                        autopool.base_asset,
                    ],
                    [("buy_token_price", norm_function)],
                )

                sell_token_price_call = Call(
                    ROOT_PRICE_ORACLE(autopool.chain),
                    [
                        "getPriceInQuote(address,address)(uint256)",
                        tokemak_quote_response["sellToken"],
                        autopool.base_asset,
                    ],
                    [("sell_token_price", norm_function)],
                )

                prices = get_state_by_one_block([buy_token_price_call, sell_token_price_call], block, autopool.chain)
                tokemak_quote_response.update(prices)
                tokemak_quote_response["block"] = block
                tokemak_quote_response["found_timestamp"] = found_timestamp.timestamp
                tokemak_quote_response["prices_success"] = True
                return tokemak_quote_response

            except Exception as e:
                tokemak_quote_response["prices_success"] = False
                i += 1
                if i < 3:
                    print("failed a row", type(e), str(e), "sleeping and retrying")

                    time.sleep(2**i * (random.random() + 0.5))

            return tokemak_quote_response

    return _fetch_on_chain_spot_prices


def fetch_swap_matrix_quotes_and_prices(
    _fetch_on_chain_spot_prices_function, tokemak_quote_request: TokemakQuoteRequest
) -> dict:
    one_quote_response = fetch_single_swap_quote_from_internal_api(
        chain_id=tokemak_quote_request.chain_id,
        sell_token=tokemak_quote_request.token_in,  # sell token == buy token
        buy_token=tokemak_quote_request.token_out,
        unscaled_amount_in=tokemak_quote_request.unscaled_amount_in,
    )

    if not one_quote_response[THIRD_PARTY_SUCCESS_KEY]:
        return one_quote_response

    else:
        one_quote_response_with_prices = _fetch_on_chain_spot_prices_function(one_quote_response)
        return one_quote_response_with_prices


def build_quotes(autopool: AutopoolConstants) -> list[TokemakQuoteRequest]:
    autopool_assets = get_autopool_possible_assets(autopool)
    if autopool.base_asset in [DOLA(autopool.chain), USDC(autopool.chain), EURC(autopool.chain), USDT(autopool.chain)]:
        sizes = [50_000, 100_000, 150_000, 200_000, 250_000]
    else:
        sizes = [5, 25, 50, 75, 100]

    tokemak_quote_requests = []

    for size in sizes:
        for chain_id, token_address1, decimals in zip(
            autopool_assets["chain_id"],
            autopool_assets["token_address"],
            autopool_assets["decimals"],
        ):
            for token_address2 in autopool_assets["token_address"]:
                if token_address1 != token_address2:
                    tokemak_quote_requests.append(
                        TokemakQuoteRequest(
                            chain_id=chain_id,
                            token_in=token_address1,
                            token_out=token_address2,
                            unscaled_amount_in=size * (10**decimals),
                        )
                    )

    random.shuffle(tokemak_quote_requests)
    return tokemak_quote_requests


@time_decorator
def main():
    bad_autopools = [BASE_EUR, SILO_ETH, SONIC_USD, BAL_ETH, DINERO_ETH, ARB_USD, SILO_USD, AUTO_LRT, ARB_USD]

    swap_matrix_data2 = WORKING_DATA_DIR / "swap_matrix_prices2"
    swap_matrix_data2.mkdir(parents=True, exist_ok=True)

    while True:
        for autopool in ALL_AUTOPOOLS:
            if autopool not in bad_autopools:
                tokemak_quote_requests = build_quotes(autopool)

                _fetch_on_chain_spot_prices = build_fetch_on_chain_spot_prices_function(autopool)

                def process_request(tokemak_quote_request):
                    time.sleep(1)
                    data = fetch_swap_matrix_quotes_and_prices(_fetch_on_chain_spot_prices, tokemak_quote_request)
                    return data

                max_workers = 10
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    quote_responses = list(
                        tqdm(
                            executor.map(process_request, tokemak_quote_requests),
                            total=len(tokemak_quote_requests),
                            desc=f"Fetching quotes for {autopool.name}",
                        )
                    )
                quote_df = pd.DataFrame.from_records(quote_responses)
                quote_df["autopool_name"] = autopool.name
                quote_df["max_workers"] = max_workers
                print(autopool.name + "-" * 60)
                print(quote_df[THIRD_PARTY_SUCCESS_KEY].value_counts())
                autopool_save_name = swap_matrix_data2 / f"{autopool.name}_full_swap_matrix_with_prices.csv"

                prior_df = pd.read_csv(autopool_save_name, low_memory=False) if autopool_save_name.exists() else None

                if prior_df is not None:
                    full_df = pd.concat([prior_df, quote_df])
                else:
                    full_df = quote_df

                full_df.to_csv(autopool_save_name, index=False)
                print("- -" * 100)
                print(f"Saved a total {len(full_df)} quotes to {autopool_save_name} {len(quote_df)} new")


if __name__ == "__main__":
    # print("hello_world")
    main()


# caffeinate -i bash -c "cd /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard && poetry run python mainnet_launch/database/schema/ensure_tables_are_current/using_3rd_party/swap_matrix_quotes_and_onchain_prices.py"
