"""Using dex screener and coingecko, fetch the USD liquidty (in the other side) of found liquidity pools"""

from web3 import Web3
import pandas as pd
import streamlit as st

from mainnet_launch.constants.constants import (
    TokemakAddress,
    ChainData,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.exit_liquidity.update_asset_exposure import (
    fetch_latest_asset_exposure,
    ensure_asset_exposure_is_current,
)
from mainnet_launch.data_fetching.dex_screener.get_pool_usd_liqudity import (
    get_many_pairs_from_dex_screener,
    get_liquidity_quantities_of_many_pools,
)

from mainnet_launch.data_fetching.coingecko.get_pools_by_token import (
    fetch_token_prices_from_coingecko,
    fetch_many_pairs_from_coingecko,
)

from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import (
    fetch_raw_amounts_by_destination,
    get_pools_underlying_and_total_supply,
)


def _fetch_possible_pairs_from_dex_screener_and_coingecko(
    tokens_we_want_the_pairs_for: list[str], chain: ChainData
) -> list[str]:

    coingecko_pair_df = fetch_many_pairs_from_coingecko(tokens_we_want_the_pairs_for, chain)
    dex_screener_df = get_many_pairs_from_dex_screener(
        chain=chain,
        token_addresses=tokens_we_want_the_pairs_for,
    )

    coingecko_pair_df["pair_address"] = coingecko_pair_df["id"].apply(lambda x: x.split("_")[1])
    coingecko_pairs = coingecko_pair_df["pair_address"].to_list()
    dex_screener_pairs = dex_screener_df["pairAddress"].to_list()
    all_pairs = set(coingecko_pairs + dex_screener_pairs)

    valid_pool_addresses = []
    for p in all_pairs:
        try:
            valid_pool_addresses.append(Web3.toChecksumAddress(p))
        except ValueError:
            # I don't like this format
            # reject non checksum addresses
            continue

    return valid_pool_addresses


def _fetch_pairs_with_prices(
    tokens_to_check_exit_liqudity_for: list[str],
    chain: ChainData,
    base_asset: TokemakAddress,
) -> pd.DataFrame:
    # this does find scrvUSD:USDC on base
    valid_pool_addresses = _fetch_possible_pairs_from_dex_screener_and_coingecko(
        tokens_to_check_exit_liqudity_for, chain
    )

    dex_df = get_liquidity_quantities_of_many_pools(chain, valid_pool_addresses)

    valid_dex_df = dex_df.dropna(subset=["liquidity_quote", "liquidity_base"]).copy()

    # need to edit this for
    ETH_to_weth = {"0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"}
    # replace all ETH with WETH
    valid_dex_df["quote_token_address"] = valid_dex_df["quote_token_address"].replace(ETH_to_weth)
    valid_dex_df["base_token_address"] = valid_dex_df["base_token_address"].replace(ETH_to_weth)

    prices_to_fetch = set(valid_dex_df["quote_token_address"].tolist() + valid_dex_df["base_token_address"].tolist())

    coingecko_prices = fetch_token_prices_from_coingecko(chain, list(prices_to_fetch))
    coingecko_prices = coingecko_prices.set_index("token_address")["usd_price"].to_dict()

    valid_dex_df["base_token_price_usd"] = valid_dex_df["base_token_address"].map(coingecko_prices)
    valid_dex_df["quote_token_price_usd"] = valid_dex_df["quote_token_address"].map(coingecko_prices)

    valid_dex_df["base_token_usd_liquidity"] = (
        valid_dex_df["liquidity_base"] * valid_dex_df["base_token_price_usd"]
    ).round()
    valid_dex_df["quote_token_usd_liquidity"] = (
        valid_dex_df["liquidity_quote"] * valid_dex_df["quote_token_price_usd"]
    ).round()

    valid_dex_df = valid_dex_df.drop_duplicates(subset=["pairAddress", "base_token_address", "quote_token_address"])

    valid_dex_df["chain_id"] = chain.chain_id
    valid_dex_df["reference_asset"] = base_asset(chain)

    return valid_dex_df, coingecko_prices


def build_our_token_to_total_other_token_liquidity(
    valid_dex_df: pd.DataFrame, tokens_to_check_exit_liqudity_for: list[str]
):
    our_token_to_total_other_token_liquidity = {}
    token_symbol_to_dfs = {}

    for token in tokens_to_check_exit_liqudity_for:
        # also works by token address
        sub_df = valid_dex_df[
            (valid_dex_df["base_token_address"] == token) | (valid_dex_df["quote_token_address"] == token)
        ]
        if sub_df.empty:
            continue

        quote_token_is_target = sub_df[(sub_df["quote_token_address"] == token)]
        quote_liqudity = quote_token_is_target.groupby("base_token_symbol")["base_token_usd_liquidity"].sum().to_dict()

        base_token_is_target = sub_df[(sub_df["base_token_address"] == token)]
        base_liqudity = base_token_is_target.groupby("quote_token_symbol")["quote_token_usd_liquidity"].sum().to_dict()

        for k, v in base_liqudity.items():
            if k not in quote_liqudity:
                quote_liqudity[k] = 0
            quote_liqudity[k] += v

        our_token_symbol = (
            quote_token_is_target["quote_token_symbol"].iloc[0]
            if not quote_token_is_target.empty
            else base_token_is_target["base_token_symbol"].iloc[0]
        )

        our_token_to_total_other_token_liquidity[our_token_symbol] = quote_liqudity
        token_symbol_to_dfs[our_token_symbol] = sub_df

    return our_token_to_total_other_token_liquidity, token_symbol_to_dfs


def fetch_percent_ownership_by_destination_from_destination_vaults(block: int, chain: ChainData) -> pd.DataFrame:
    df = fetch_raw_amounts_by_destination(
        block=block,
        chain=chain,
    )

    states = get_pools_underlying_and_total_supply(
        destination_vaults=df["vault_address"].unique(),
        block=block,
        chain=chain,
    )

    records = {}
    for (vault_address, key), value in states.items():
        if vault_address not in records:
            records[vault_address] = {}
        records[vault_address][key] = str(value)

    portion_ownership_by_destination_df = pd.DataFrame.from_dict(records, orient="index").reset_index()

    portion_ownership_by_destination_df["portion_ownership"] = portion_ownership_by_destination_df.apply(
        lambda row: int(row["totalSupply"]) / int(row["underlyingTotalSupply"]), axis=1
    )
    portion_ownership_by_destination_df["percent_ownership"] = (
        portion_ownership_by_destination_df["portion_ownership"] * 100
    ).round(2)

    return portion_ownership_by_destination_df


def _downscale_usd_liquidity_by_portion_ownership(
    valid_dex_df: pd.DataFrame, pool_to_portion_ownership: pd.DataFrame
) -> pd.DataFrame:

    valid_dex_df["tokemak_portion_ownership"] = (
        valid_dex_df["pairAddress"].str.lower().map(pool_to_portion_ownership).fillna(0.0)
    )
    valid_dex_df["tokemak_percent_ownership"] = (valid_dex_df["tokemak_portion_ownership"] * 100).round(2)

    valid_dex_df["scaled_quote_usd_liquidity"] = valid_dex_df["quote_token_usd_liquidity"] * (
        1 - valid_dex_df["tokemak_portion_ownership"]
    )
    valid_dex_df["scaled_base_usd_liquidity"] = valid_dex_df["base_token_usd_liquidity"] * (
        1 - valid_dex_df["tokemak_portion_ownership"]
    )
    return valid_dex_df


def _pure_function_group_destinations(
    all_chain_asset_exposure_df: pd.DataFrame, chain: ChainData, base_asset: TokemakAddress
):
    this_chain_and_base_asset_exposure_df = all_chain_asset_exposure_df[
        (all_chain_asset_exposure_df["chain_id"] == chain.chain_id)
        & (all_chain_asset_exposure_df["reference_asset"] == base_asset(chain))
    ]

    tokens_to_check_exit_liqudity_for = [
        t for t in this_chain_and_base_asset_exposure_df["token_address"].unique().tolist() if t != base_asset(chain)
    ]

    portion_ownership_by_destination_df = fetch_percent_ownership_by_destination_from_destination_vaults(
        chain.client.eth.block_number, chain
    )
    pool_to_portion_ownership = portion_ownership_by_destination_df.set_index("getPool")["portion_ownership"].to_dict()

    valid_dex_df, coingecko_prices = _fetch_pairs_with_prices(tokens_to_check_exit_liqudity_for, chain, base_asset)

    valid_dex_df = _downscale_usd_liquidity_by_portion_ownership(valid_dex_df, pool_to_portion_ownership)

    our_token_to_total_other_token_liquidity, token_symbol_to_dfs = build_our_token_to_total_other_token_liquidity(
        valid_dex_df, tokens_to_check_exit_liqudity_for
    )

    return (
        valid_dex_df,
        all_chain_asset_exposure_df,
        our_token_to_total_other_token_liquidity,
        token_symbol_to_dfs,
        portion_ownership_by_destination_df,
        coingecko_prices,
    )


def fetch_exit_liqudity_tvl(chain: ChainData, base_asset: TokemakAddress, refresh: bool):
    if refresh:
        ensure_asset_exposure_is_current()  # 12 seconds

    all_chain_asset_exposure_df = fetch_latest_asset_exposure()

    (
        valid_dex_df,
        all_chain_asset_exposure_df,
        our_token_to_total_other_token_liquidity,
        token_symbol_to_dfs,
        portion_ownership_by_destination_df,
        coingecko_prices,
    ) = _pure_function_group_destinations(  # not a pure function
        all_chain_asset_exposure_df=all_chain_asset_exposure_df, chain=chain, base_asset=base_asset
    )

    return (
        valid_dex_df,
        all_chain_asset_exposure_df,
        our_token_to_total_other_token_liquidity,
        token_symbol_to_dfs,
        portion_ownership_by_destination_df,
        coingecko_prices,
    )
