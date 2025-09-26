import time
import random
import pandas as pd

from mainnet_launch.constants import *

from mainnet_launch.database.postgres_operations import (
    _exec_sql_and_cache,
)

from mainnet_launch.data_fetching.internal.fetch_quotes import (
    fetch_many_swap_quotes_from_internal_api,
    TokemakQuoteRequest,
    THIRD_PARTY_SUCCESS_KEY,
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

    return _exec_sql_and_cache(query)


def fetch_and_save_autopool_swap_matrix_quotes(autopool: AutopoolConstants):
    autopool_save_name = WORKING_DATA_DIR / f"{autopool.name}_full_swap_matrix.csv"
    autopool_assets = get_autopool_possible_assets(autopool)

    print(
        f'Autopool {autopool.name} has {len(autopool_assets)} possible assets: {", ".join(autopool_assets["symbol"].unique())}'
    )

    if autopool.base_asset in [DOLA(autopool.chain), USDC(autopool.chain), EURC(autopool.chain)]:
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
    quote_df = fetch_many_swap_quotes_from_internal_api(
        tokemak_quote_requests,
        rate_limit_max_rate=8,
        rate_limit_time_period=10,
    )
    quote_df["autopool_name"] = autopool.name

    print("fetched quotes")
    print(quote_df[THIRD_PARTY_SUCCESS_KEY].value_counts())

    prior_df = pd.read_csv(autopool_save_name) if autopool_save_name.exists() else None

    if prior_df is not None:
        full_df = pd.concat([prior_df, quote_df])
    else:
        full_df = quote_df

    full_df.to_csv(autopool_save_name, index=False)

    print(f"Saved a total {len(full_df)} quotes to {autopool_save_name} {len(quote_df)} new")


def main():
    # not sure why arb USD fails, it shouldn't
    # the rest are redundent, there is only one extra asset that justifices inlucidng autoLRT
    # can push into autoETH if needed
    bad_autopools = [BASE_EUR, SILO_ETH, SONIC_USD, BAL_ETH, DINERO_ETH, ARB_USD, SILO_USD]

    while True:
        for autopool in ALL_AUTOPOOLS:
            if autopool not in bad_autopools:
                print(f"Fetching quotes for {autopool.name}")
                fetch_and_save_autopool_swap_matrix_quotes(autopool)
                print("sleeping 5 minutes")
                time.sleep(60 * 5)


if __name__ == "__main__":
    main()
