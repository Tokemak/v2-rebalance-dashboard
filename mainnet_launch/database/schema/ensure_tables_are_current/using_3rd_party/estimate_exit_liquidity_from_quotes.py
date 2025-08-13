"""
Takes 5 minutes ot run, with curernt setup

"""

import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from mainnet_launch.constants import *
from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination
from mainnet_launch.data_fetching.block_timestamp import ensure_all_blocks_are_in_table
from mainnet_launch.pages.risk_metrics.percent_ownership_by_destination import (
    fetch_readable_our_tvl_by_destination,
)

from mainnet_launch.database.schema.full import (
    AutopoolDestinations,
    Tokens,
    SwapQuote,
    AssetExposure,
    ENGINE,
)
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_df,
    insert_avoid_conflicts,
    get_highest_value_in_field_where,
)

from mainnet_launch.data_fetching.internal.fetch_quotes import (
    fetch_many_swap_quotes_from_internal_api,
    TokemakQuoteRequest,
)
from mainnet_launch.data_fetching.odos.fetch_quotes import fetch_many_odos_raw_quotes, OdosQuoteRequest

ENGINE.echo = False


CHAIN_BASE_ASSET_GROUPS = {
    (ETH_CHAIN, WETH): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH),
    (ETH_CHAIN, USDC): (AUTO_USD,),
    (ETH_CHAIN, DOLA): (AUTO_DOLA,),
    (SONIC_CHAIN, USDC): (SONIC_USD,),
    (BASE_CHAIN, WETH): (BASE_ETH,),
    (BASE_CHAIN, USDC): (BASE_USD,),
}


ATTEMPTS = 3
# large nubmers me we don't exclude it
PERCENT_OWNERSHIP_THRESHOLD = 99  # what percent of a pool can we do we own before we exclude it from odos quotes

STABLE_COINS_REFERENCE_QUANTITY = 10_000
ETH_REFERENCE_QUANTITY = 5

USD_SCALED_SIZES = [i * 100_000 for i in range(1, 11)]  #
USD_SCALED_SIZES.append(STABLE_COINS_REFERENCE_QUANTITY)
ETH_SCALED_SIZES = [i * 25 for i in range(1, 16)]
ETH_SCALED_SIZES.append(ETH_REFERENCE_QUANTITY)

# smaller for testing
# USD_SCALED_SIZES = [STABLE_COINS_REFERENCE_QUANTITY]
# ETH_SCALED_SIZES = [ETH_REFERENCE_QUANTITY]


def _fetch_current_asset_exposure(
    chain: ChainData, valid_autopools: list[AutopoolConstants], block: int
) -> dict[str, int]:
    """Fetches the exposure and pools to exclude for the given chain and base asset."""
    reserve_df = fetch_raw_amounts_by_destination(block, chain)
    valid_autopool_symbols = [autopool.symbol for autopool in valid_autopools]

    reserve_df = reserve_df[reserve_df["autopool_symbol"].isin(valid_autopool_symbols)].copy()
    reserve_df["reserve_amount"] = reserve_df["reserve_amount"].map(int)
    unscaled_asset_exposure = reserve_df.groupby("token_address")["reserve_amount"].sum().to_dict()
    return unscaled_asset_exposure


def fetch_needed_context(chain: ChainData, valid_autopools: list[AutopoolConstants]):
    block = chain.client.eth.block_number
    # TODO I suspect this duplicates work
    unscaled_asset_exposure = _fetch_current_asset_exposure(chain, valid_autopools, block)
    percent_ownership_by_destination_df = fetch_readable_our_tvl_by_destination(chain, block)

    autopool_destinations = get_full_table_as_df(
        AutopoolDestinations,
        where_clause=AutopoolDestinations.autopool_vault_address.in_(a.autopool_eth_addr for a in valid_autopools),
    )
    these_autopools_destinations = autopool_destinations["destination_vault_address"].unique().tolist()
    percent_ownership_by_destination_df = percent_ownership_by_destination_df[
        percent_ownership_by_destination_df["destination_vault_address"].isin(these_autopools_destinations)
    ].copy()

    return unscaled_asset_exposure, percent_ownership_by_destination_df


def _build_quote_requests_from_absolute_sizes(
    chain: ChainData,
    base_asset: TokemakAddress,
    unscaled_asset_exposure: dict[str, int],
    percent_ownership_by_destination_df: pd.DataFrame,
    token_df: pd.DataFrame,
) -> tuple[list[TokemakQuoteRequest], list[OdosQuoteRequest]]:

    token_to_decimal = token_df.set_index("token_address")["decimals"].to_dict()

    tokemak_quote_requests = []
    odos_quote_requests = []

    poolBlacklist = (
        percent_ownership_by_destination_df[
            percent_ownership_by_destination_df["percent_ownership"] > PERCENT_OWNERSHIP_THRESHOLD
        ]["pool_address"]
        .unique()
        .tolist()
    )

    if base_asset(chain) == WETH(chain):
        sizes = ETH_SCALED_SIZES
    elif (base_asset(chain) == USDC(chain)) or (base_asset(chain) == DOLA(chain)):  # add EURC here
        sizes = USD_SCALED_SIZES
    else:
        raise ValueError(f"Unexpected base asset: {base_asset.name}")

    for size in sizes:
        for token_address, _ in unscaled_asset_exposure.items():
            if token_address == base_asset(chain):
                # Skip the base asset itself, as we don't need to quote it against itself
                continue

            decimals = token_to_decimal[token_address]
            unscaled_amount_times_size = int(size * 10**decimals)

            tokemak_quote_requests.append(
                TokemakQuoteRequest(
                    chain_id=chain.chain_id,
                    token_in=token_address,
                    token_out=base_asset(chain),
                    unscaled_amount_in=unscaled_amount_times_size,
                )
            )

            odos_quote_requests.append(
                OdosQuoteRequest(
                    chain_id=chain.chain_id,
                    token_in=token_address,
                    token_out=base_asset(chain),
                    unscaled_amount_in=unscaled_amount_times_size,
                    poolBlacklist=poolBlacklist,
                )
            )

    return tokemak_quote_requests, odos_quote_requests


def _post_process_raw_tokemak_quote_response_df(
    raw_tokemak_quote_response_df: pd.DataFrame,
    token_df: pd.DataFrame,
    batch_id: int,
    base_asset: str,
) -> list[SwapQuote]:
    token_to_decimal = token_df.set_index("token_address")["decimals"].to_dict()

    quotes: list[SwapQuote] = []
    for _, row in raw_tokemak_quote_response_df.iterrows():

        sell_addr = Web3.toChecksumAddress(row["sellToken"])
        buy_addr = Web3.toChecksumAddress(row["buyToken"])
        chain_id = int(row["chainId"])

        # scale raw integer amounts by the token decimals
        scaled_in = int(row["sellAmount"]) / 10 ** token_to_decimal[sell_addr]
        scaled_out = int(row["buyAmount"]) / 10 ** token_to_decimal[buy_addr]

        quote = SwapQuote(
            chain_id=chain_id,
            api_name="tokemak",
            sell_token_address=sell_addr,
            buy_token_address=buy_addr,
            scaled_amount_in=scaled_in,
            scaled_amount_out=scaled_out,
            pools_blacklist=None,
            aggregator_name=row["aggregatorName"],
            datetime_received=row["datetime_received"],
            quote_batch=batch_id,
            base_asset=base_asset,
            percent_exclude_threshold=PERCENT_OWNERSHIP_THRESHOLD,
        )
        quotes.append(quote)

    return quotes


def _post_process_raw_odos_quote_response_df(
    raw_odos_quote_response_df: pd.DataFrame,
    token_df: pd.DataFrame,
    batch_id: int,
    base_asset: str,
) -> list[SwapQuote]:
    token_to_decimal = token_df.set_index("token_address")["decimals"].to_dict()
    quotes: list[SwapQuote] = []

    for _, row in raw_odos_quote_response_df.iterrows():
        try:
            sell_addr = Web3.toChecksumAddress(row["inTokens"])
            buy_addr = Web3.toChecksumAddress(row["outTokens"])
            chain_id = int(row["chainId"])

            unscaled_in = int(row["inAmounts"])
            unscaled_out = int(row["outAmounts"])

            scaled_in = unscaled_in / 10 ** token_to_decimal[sell_addr]
            scaled_out = unscaled_out / 10 ** token_to_decimal[buy_addr]

            blacklist = row.get("poolBlacklist", "")

            quote = SwapQuote(
                chain_id=chain_id,
                api_name="odos",
                sell_token_address=sell_addr,
                buy_token_address=buy_addr,
                scaled_amount_in=scaled_in,
                scaled_amount_out=scaled_out,
                pools_blacklist=str(blacklist),
                aggregator_name="Odos",
                datetime_received=row["datetime_received"],
                quote_batch=batch_id,
                base_asset=base_asset,
                percent_exclude_threshold=PERCENT_OWNERSHIP_THRESHOLD,
            )
            quotes.append(quote)
        except Exception as e:
            with open("error_log.txt", "a") as f:
                f.write(f"Error processing Odos quote: {e}\n {type(e)}")

    return quotes


def _extract_asset_exposure_rows(
    chain: ChainData,
    base_asset: TokemakAddress,
    block: int,
    unscaled_asset_exposure: dict[str, int],
    tokens: pd.DataFrame,
    highest_swap_quote_batch_id: int,
) -> list[AssetExposure]:
    scaled_asset_exposure = {
        token: amount / 10 ** tokens[tokens["token_address"] == token]["decimals"].values[0]
        for token, amount in unscaled_asset_exposure.items()
    }
    new_asset_exposure_rows = []
    for token_address, scaled_amount in scaled_asset_exposure.items():
        asset_exposure = AssetExposure(
            block=block,
            chain_id=chain.chain_id,
            token_address=Web3.toChecksumAddress(token_address),
            reference_asset=base_asset(chain),
            quantity=scaled_amount,
            quote_batch=highest_swap_quote_batch_id,
        )
        new_asset_exposure_rows.append(asset_exposure)

    return new_asset_exposure_rows


def _build_requests_and_asset_exposure_rows():
    highest_swap_quote_batch_id = get_highest_value_in_field_where(SwapQuote, SwapQuote.quote_batch, where_clause=None)
    if highest_swap_quote_batch_id is None:
        highest_swap_quote_batch_id = 0
    else:
        highest_swap_quote_batch_id += 1

    asset_expoosure_rows = []
    token_df = get_full_table_as_df(Tokens, where_clause=None)

    all_tokemak_requests = []
    all_odos_requests = []
    for k, valid_autopools in CHAIN_BASE_ASSET_GROUPS.items():
        chain, base_asset = k

        unscaled_asset_exposure, percent_ownership_by_destination_df = fetch_needed_context(chain, valid_autopools)
        new_asset_exposure_rows = _extract_asset_exposure_rows(
            chain,
            base_asset,
            chain.client.eth.block_number,
            unscaled_asset_exposure,
            token_df,
            highest_swap_quote_batch_id,
        )
        asset_expoosure_rows.extend(new_asset_exposure_rows)

        tokemak_requests, odos_requests = _build_quote_requests_from_absolute_sizes(
            chain, base_asset, unscaled_asset_exposure, percent_ownership_by_destination_df, token_df
        )

        all_tokemak_requests.extend(tokemak_requests)
        all_odos_requests.extend(odos_requests)

    # we want 3 of each request to get the median
    all_tokemak_requests = all_tokemak_requests * ATTEMPTS
    all_odos_requests = all_odos_requests * ATTEMPTS

    return all_tokemak_requests, all_odos_requests, highest_swap_quote_batch_id, asset_expoosure_rows, token_df


def _fetch_all_quotes(
    all_tokemak_requests,
    all_odos_requests,
):
    with ThreadPoolExecutor(max_workers=2) as executor:
        # this is deliberately rate limited to be slow
        # this is to avoid hitting the API rate limits

        # we don't know what the rate limit is for the tokemak API
        # it will silently fail to return a quote from one of the aggregators if that version fails
        future_tokemak = executor.submit(fetch_many_swap_quotes_from_internal_api, all_tokemak_requests)
        future_odos = executor.submit(fetch_many_odos_raw_quotes, all_odos_requests)

        tokemak_quote_response_df = future_tokemak.result()
        odos_quote_response_df = future_odos.result()

    return tokemak_quote_response_df, odos_quote_response_df


def insert_new_batch_quotes(
    odos_quote_response_df: pd.DataFrame,
    tokemak_quote_response_df: pd.DataFrame,
    token_df: pd.DataFrame,
    highest_swap_quote_batch_id: int,
):
    processed_quotes = []

    for k, _ in CHAIN_BASE_ASSET_GROUPS.items():
        chain, base_asset = k

        sub_odos_df = odos_quote_response_df[
            (odos_quote_response_df["chainId"] == chain.chain_id)
            & (odos_quote_response_df["outTokens"].apply(lambda x: Web3.toChecksumAddress(x)) == base_asset(chain))
        ].reset_index(drop=True)

        sub_tokemak_df = tokemak_quote_response_df[
            (tokemak_quote_response_df["chainId"] == chain.chain_id)
            & (tokemak_quote_response_df["buyToken"] == base_asset(chain))
        ].reset_index(drop=True)

        cleaned_odos_responses = _post_process_raw_odos_quote_response_df(
            sub_odos_df, token_df, highest_swap_quote_batch_id, base_asset(chain)
        )
        cleaned_tokemak_responses = _post_process_raw_tokemak_quote_response_df(
            sub_tokemak_df, token_df, highest_swap_quote_batch_id, base_asset(chain)
        )

        processed_quotes.extend(cleaned_odos_responses)
        processed_quotes.extend(cleaned_tokemak_responses)

    insert_avoid_conflicts(
        processed_quotes,
        SwapQuote,
    )


@time_decorator
def fetch_and_save_all_at_once():
    all_tokemak_requests, all_odos_requests, highest_swap_quote_batch_id, new_asset_exposure_rows, token_df = (
        _build_requests_and_asset_exposure_rows()
    )
    print(f"Fetching {len(all_odos_requests)} Odos quotes and {len(all_tokemak_requests)} Tokemak quotes.")

    tokemak_quote_response_df, odos_quote_response_df = _fetch_all_quotes(all_tokemak_requests, all_odos_requests)
    print(f"Fetched {len(tokemak_quote_response_df)} Tokemak quotes and {len(odos_quote_response_df)} Odos quotes.")

    insert_new_batch_quotes(
        odos_quote_response_df=odos_quote_response_df,
        tokemak_quote_response_df=tokemak_quote_response_df,
        token_df=token_df,
        highest_swap_quote_batch_id=highest_swap_quote_batch_id,
    )

    for chain in ALL_CHAINS:
        blocks = [r.block for r in new_asset_exposure_rows if r.chain_id == chain.chain_id]
        ensure_all_blocks_are_in_table(blocks, chain)

    insert_avoid_conflicts(
        new_asset_exposure_rows,
        AssetExposure,
    )


if __name__ == "__main__":
    for i in range(24):
        try:
            fetch_and_save_all_at_once()
            print("success")
            time.sleep(60 * 30)  # every 30 minutes
            print(f"Iteration {i + 1} completed. Waiting for the next run...")
        except Exception as e:
            print(f"Error in iteration {i + 1}: {e}")
            # write the error to a local log file
            with open("error_log.txt", "a") as f:
                f.write(f"Error in iteration {i + 1}: {e}\n")
            # wait 5 minutes before retrying
            print("Retrying in 5 minutes...")
            time.sleep(60 * 5)

# caffeinate -ims bash -c 'cd /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard && poetry run python mainnet_launch/database/schema/ensure_tables_are_current/using_3rd_party/estimate_exit_liquidity_from_quotes.py'
