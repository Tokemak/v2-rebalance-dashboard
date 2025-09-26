from mainnet_launch.constants import *

from mainnet_launch.database.postgres_operations import (
    _exec_sql_and_cache,
)


import time
import random
import pandas as pd


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


from mainnet_launch.data_fetching.internal.fetch_quotes import (
    fetch_many_swap_quotes_from_internal_api,
    TokemakQuoteRequest,
)


def tidy_up_quotes(df: pd.DataFrame, token_address_to_decimals: dict, token_address_to_symbol: dict) -> pd.DataFrame:
    df["buyAmount"] = df["buyAmount"].fillna(0)
    df["minBuyAmount"] = df["minBuyAmount"].fillna(0)
    df["sellAmount"] = df["sellAmount"].fillna(0)

    df["buy_amount_norm"] = df.apply(
        lambda row: int(row["buyAmount"]) / 10 ** token_address_to_decimals[row["buyToken"]], axis=1
    )
    df["min_buy_amount_norm"] = df.apply(
        lambda row: float(row["minBuyAmount"]) / 10 ** token_address_to_decimals[row["buyToken"]], axis=1
    )
    df["sell_amount_norm"] = df.apply(
        lambda row: int(row["sellAmount"]) / 10 ** token_address_to_decimals[row["sellToken"]], axis=1
    )

    df["buy_amount_price"] = df.apply(lambda row: row["buy_amount_norm"] / row["sell_amount_norm"], axis=1)
    df["min_buy_amount_price"] = df.apply(lambda row: row["min_buy_amount_norm"] / row["sell_amount_norm"], axis=1)
    df["buy_symbol"] = df.apply(lambda row: token_address_to_symbol[row["buyToken"]], axis=1)
    df["sell_symbol"] = df.apply(lambda row: token_address_to_symbol[row["sellToken"]], axis=1)
    df["label"] = df["sell_symbol"] + " -> " + df["buy_symbol"]
    return df


def fetch_a_bunch_of_quotes(
    tokemak_quote_requests: list[TokemakQuoteRequest],
    token_address_to_decimals: dict,
    token_address_to_symbol: dict,
    second_wait_between_batches: int = 60 * 15,
    rate_limit_max_rate=8,
    rate_limit_time_period=10,
    n_batches=10,
):

    dfs = []
    for i in range(n_batches):
        random.shuffle(tokemak_quote_requests)  # important to not have spurious relationships because of sizing
        df = fetch_many_swap_quotes_from_internal_api(
            tokemak_quote_requests,
            rate_limit_max_rate=rate_limit_max_rate,
            rate_limit_time_period=rate_limit_time_period,
        )
        df = tidy_up_quotes(df, token_address_to_decimals, token_address_to_symbol)
        df["batch_id"] = i
        dfs.append(df)

        full_df = pd.concat(dfs)
        full_df.to_csv("15_min_auto_usd_combinations6.csv", index=True)
        print(f"wrote {len(full_df)} rows to 15_min_auto_usd_combinations4.csv")

        if i < n_batches - 1:
            print(f"waiting {second_wait_between_batches} seconds before next batch")
            time.sleep(second_wait_between_batches)


def main():
    autoUSD_assets = get_autopool_possible_assets(AUTO_USD)
    erc4626_asset_symbols = [
        "waEthUSDC",
        "sUSDS",
        "sFRAX",
        "sUSDe",
        "sfrxUSD",
        "scrvUSD",
        "waEthUSDT",
        "waEthLidoGHO",
        "sDAI",
        "frxUSD",
    ]
    # in theory we can derive these prices from the other prices + an on-chain call
    # FRAX is 1:1 with frxUSD, FRAX is more liquid, it shouldn't matter but using FRAX instead
    primary_autoUSD_assets = autoUSD_assets[~autoUSD_assets["symbol"].isin(erc4626_asset_symbols)].copy()
    # it is required to reduce down to only the primary assets and then use the erc4626 method to get quanity in that assets

    token_address_to_symbol = primary_autoUSD_assets.set_index("token_address")["symbol"].to_dict()
    token_address_to_decimals = primary_autoUSD_assets.set_index("token_address")["decimals"].to_dict()

    sizes = [50_000, 100_000, 150_000, 200_000, 250_000]

    tokemak_quote_requests = []

    for size in sizes:
        for chain_id, token_address1, decimals in zip(
            primary_autoUSD_assets["chain_id"],
            primary_autoUSD_assets["token_address"],
            primary_autoUSD_assets["decimals"],
        ):
            for token_address2 in primary_autoUSD_assets["token_address"]:
                if token_address1 != token_address2:
                    tokemak_quote_requests.append(
                        TokemakQuoteRequest(
                            chain_id=chain_id,
                            token_in=token_address1,
                            token_out=token_address2,
                            unscaled_amount_in=size * (10**decimals),
                        )
                    )

    fetch_a_bunch_of_quotes(
        tokemak_quote_requests,
        token_address_to_decimals,
        token_address_to_symbol,
        second_wait_between_batches=60 * 30,
        rate_limit_max_rate=8,
        rate_limit_time_period=10,
        n_batches=50,
    )


if __name__ == "__main__":
    main()
