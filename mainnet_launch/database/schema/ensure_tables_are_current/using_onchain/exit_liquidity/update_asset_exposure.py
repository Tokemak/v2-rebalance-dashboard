"""
For each (chain, base asset) pair:

1. Fetch the latest quantity of each asset we hold across each autopool.
2. Using our swapper API, get quotes for selling back to the base asset at various sizes.
"""

import asyncio
import streamlit as st
import plotly.express as px
import pandas as pd


from mainnet_launch.constants import (
    ChainData,
    TokemakAddress,
    AutopoolConstants,
    ALL_AUTOPOOLS,
    ALL_CHAINS,
    DOLA,
    USDC,
    WETH,
)

from mainnet_launch.data_fetching.update_blocks import ensure_all_blocks_are_in_table
from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination

from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_full_table_as_orm,
    _exec_sql_and_cache,
)
from mainnet_launch.database.schema.full import AssetExposure, SwapQuote, Tokens

# TODO getting the safe and spot prices here as well,
# then you can compare the swaper with our oracles

USD_REFERENCE_QUANTITY = 10_000
ETH_REFERENCE_QUANTITY = 5

STANDARD_USD_QUANTITIES = [USD_REFERENCE_QUANTITY, 35_000, 100_000, 200_000]
STANDARD_ETH_QUANTITIES = [ETH_REFERENCE_QUANTITY, 100, 200]

PORTIONS_TO_CHECK = [0.025, 0.05, 0.1, 0.25, 1]


def _fetch_tokens_to_decimals(tokens: list[str], chain: ChainData) -> dict[str, int]:
    tokens_orms: list[Tokens] = get_full_table_as_orm(
        Tokens, where_clause=(Tokens.token_address.in_(tokens) & (Tokens.chain_id == chain.chain_id))
    )
    tokens_to_decimals = {t.token_address: t.decimals for t in tokens_orms}
    return tokens_to_decimals


def _fetch_asset_exposure(block: int, chain: ChainData, base_asset: TokemakAddress) -> list[AssetExposure]:
    """Fetch the total asset exposure for a given chain and base asset at the give block from an external source"""
    valid_autopools = [
        autopool for autopool in ALL_AUTOPOOLS if autopool.chain == chain and autopool.base_asset in base_asset
    ]
    if not valid_autopools:
        # early exit, for the combinations of (sonic, ETH), (base, DOLA) or (sonic, DOLA) ...
        return []

    reserve_df = fetch_raw_amounts_by_destination(block, chain)
    valid_autopool_symbols = [pool.symbol for pool in valid_autopools]
    # this limits by the reference asset
    reserve_df = reserve_df[reserve_df["autopool_symbol"].isin(valid_autopool_symbols)].copy()
    reserve_df["reserve_amount"] = reserve_df["reserve_amount"].map(int)
    asset_exposure = reserve_df.groupby("token_address")["reserve_amount"].sum().to_dict()

    tokens_to_decimals = _fetch_tokens_to_decimals(
        tokens=list(asset_exposure.keys()),
        chain=chain,
    )

    asset_exposure_records = []
    for token_address, raw_quantity in asset_exposure.items():
        if int(raw_quantity) == 0:
            # if we have no exposure to this token, we don't need to save it because is implictily 0
            continue

        asset_exposure_records.append(
            AssetExposure(
                chain_id=chain.chain_id,
                reference_asset=base_asset,
                token_address=token_address,
                block=block,
                quantity=raw_quantity / 10 ** tokens_to_decimals[token_address],
            )
        )
    # this is in raw (1e18 intstead of 1) for WETH
    # raw_quantities
    return asset_exposure_records


def _write_asset_exposure_to_database(asset_exposure_records: list[AssetExposure], chain: ChainData):
    block = asset_exposure_records[0].block
    ensure_all_blocks_are_in_table([block], chain)
    insert_avoid_conflicts(asset_exposure_records, AssetExposure, index_elements=None)
    return asset_exposure_records


def ensure_asset_exposure_is_current():
    for chain in ALL_CHAINS:
        all_asset_exposure_records = []
        for base_asset in [DOLA, USDC, WETH]:
            asset_exposure_records = _fetch_asset_exposure(
                block=chain.get_block_near_top(),
                chain=chain,
                base_asset=base_asset(chain),
            )
            if not asset_exposure_records:
                continue

            all_asset_exposure_records.extend(asset_exposure_records)

        _write_asset_exposure_to_database(all_asset_exposure_records, chain)


def fetch_latest_asset_exposure() -> pd.DataFrame:
    # not totally certain on this apparoach, it is in plain sql,
    # so it is brittle to changes in the schema
    # but is is easy to read, keeping as is, as an expirement

    query = """
WITH latest_blocks AS (
  SELECT
    chain_id,
    reference_asset,
    MAX(block) AS highest_block
  FROM asset_exposure
  GROUP BY chain_id, reference_asset
)
SELECT
  ae.chain_id,
  ae.reference_asset,
  tref.symbol     AS reference_symbol,
  ae.block,
  ae.token_address,
  t.symbol        AS token_symbol,
  ae.quantity
FROM asset_exposure ae
JOIN latest_blocks lb
  ON ae.chain_id        = lb.chain_id
 AND ae.reference_asset = lb.reference_asset
 AND ae.block           = lb.highest_block
JOIN tokens t
  ON ae.token_address   = t.token_address
 AND ae.chain_id        = t.chain_id
JOIN tokens tref
  ON ae.reference_asset = tref.token_address
 AND ae.chain_id        = tref.chain_id
    """

    df = _exec_sql_and_cache(query)
    return df


if __name__ == "__main__":
    ensure_asset_exposure_is_current()
    df = fetch_latest_asset_exposure()
    pass
