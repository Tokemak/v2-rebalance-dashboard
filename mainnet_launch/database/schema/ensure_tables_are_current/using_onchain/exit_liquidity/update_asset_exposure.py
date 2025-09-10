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
    ALL_BASE_ASSETS,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    ensure_all_blocks_are_in_table,
)
from mainnet_launch.database.views import get_token_details_dict
from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination

from mainnet_launch.database.postgres_operations import (
    insert_avoid_conflicts,
    get_full_table_as_orm,
    _exec_sql_and_cache,
)
from mainnet_launch.database.schema.full import AssetExposure


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

    token_to_decimals, token_to_symbol = get_token_details_dict()

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
                quantity=raw_quantity / 10 ** token_to_decimals[token_address],
            )
        )
    return asset_exposure_records


def _write_asset_exposure_to_database(asset_exposure_records: list[AssetExposure], chain: ChainData):
    block = asset_exposure_records[0].block
    ensure_all_blocks_are_in_table([block], chain)
    insert_avoid_conflicts(asset_exposure_records, AssetExposure, index_elements=None)
    return asset_exposure_records


def ensure_asset_exposure_is_current():
    for chain in ALL_CHAINS:
        all_asset_exposure_records = []
        for base_asset in ALL_BASE_ASSETS:
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
