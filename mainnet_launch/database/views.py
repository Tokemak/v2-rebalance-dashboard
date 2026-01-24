"""Not exactly views but the same concept"""

import pandas as pd


from mainnet_launch.constants import *

from sqlalchemy import text
from mainnet_launch.database.schema.full import *
from mainnet_launch.database.postgres_operations import (
    merge_tables_as_df,
    get_full_table_as_df,
    TableSelector,
    Session,
    _exec_sql_and_cache,
)


def get_latest_rebalance_event_datetime_for_autopool(autopool: AutopoolConstants) -> pd.Timestamp | None:
    """
    Return the datetime from the blocks table for the highest block in rebalance_events
    for the given autopool_vault_address. Ensures the datetime is explicitly cast to timestamptz.
    """
    sql_txt = """
        SELECT CAST(b.datetime AS timestamptz) AS datetime
        FROM rebalance_events AS re
        JOIN transactions     AS t ON t.tx_hash  = re.tx_hash
        JOIN blocks           AS b ON b.block    = t.block
                                   AND b.chain_id = t.chain_id
        WHERE re.autopool_vault_address = :autopool_vault_address
        ORDER BY t.block DESC
        LIMIT 1;
    """
    with Session.begin() as session:
        result = session.execute(
            text(sql_txt), {"autopool_vault_address": autopool.autopool_eth_addr}
        ).scalar_one_or_none()
        return pd.Timestamp(result) if result is not None else None


def fetch_rich_autopool_destinations_table() -> pd.DataFrame:
    """Returns autopool destinations with autopool name and destination info merged in"""
    query = """
            SELECT
        d.*,
        
        ap.name AS autopool_name,
        ap.base_asset as autopool_base_asset
        
        FROM autopool_destinations AS ad
        LEFT JOIN autopools AS ap
        ON ap.autopool_vault_address = ad.autopool_vault_address
        LEFT JOIN destinations AS d
        ON d.destination_vault_address = ad.destination_vault_address
        AND d.chain_id = ad.chain_id;
    """
    df = _exec_sql_and_cache(query)
    return df


def get_all_autopool_destinations(autopool: AutopoolConstants) -> pd.DataFrame:
    return merge_tables_as_df(
        selectors=[
            TableSelector(
                AutopoolDestinations,
                select_fields=[],
                row_filter=AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr,
            ),
            TableSelector(
                Destinations,
                join_on=Destinations.destination_vault_address == AutopoolDestinations.destination_vault_address,
            ),
        ]
    )


def get_all_autopool_basket_of_primary_assets(autopool: AutopoolConstants) -> pd.DataFrame:
    """Gets the rows in tokens, that are an underlying token in autopool, eg USDC, DOLA, sDOLA ... in sDOLA"""
    df = merge_tables_as_df(
        selectors=[
            TableSelector(DestinationTokens, select_fields=[]),
            TableSelector(
                Tokens,
                join_on=(DestinationTokens.chain_id == Tokens.chain_id)
                & (DestinationTokens.token_address == Tokens.token_address),
            ),
            TableSelector(
                AutopoolDestinations,
                select_fields=[],
                row_filter=AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr,
                join_on=(DestinationTokens.destination_vault_address == AutopoolDestinations.destination_vault_address),
            ),
        ]
    )
    return df.drop_duplicates()


def fetch_autopool_destination_state_df(autopool: AutopoolConstants) -> pd.DataFrame:
    """Gets TVL, prices, shares and APR data for each destintion for this autopool"""

    destinations_df = merge_tables_as_df(
        [
            TableSelector(
                table=DestinationTokenValues,
                select_fields=[
                    DestinationTokenValues.token_address,
                    DestinationTokenValues.destination_vault_address,
                    DestinationTokenValues.quantity,
                    DestinationTokenValues.block,
                ],
            ),
            TableSelector(
                table=Tokens,
                select_fields=[Tokens.symbol],
                join_on=(Tokens.token_address == DestinationTokenValues.token_address)
                & (Tokens.chain_id == DestinationTokenValues.chain_id),
            ),
            TableSelector(
                table=TokenValues,
                select_fields=[
                    TokenValues.safe_price,
                    TokenValues.denominated_in,
                    TokenValues.backing,
                ],
                join_on=(TokenValues.chain_id == DestinationTokenValues.chain_id)
                & (TokenValues.token_address == DestinationTokenValues.token_address)
                & (TokenValues.block == DestinationTokenValues.block)
                & (TokenValues.token_address == DestinationTokenValues.token_address),
            ),
            TableSelector(
                table=AutopoolDestinationStates,
                select_fields=[
                    AutopoolDestinationStates.owned_shares,
                ],
                join_on=(DestinationTokenValues.chain_id == AutopoolDestinationStates.chain_id)
                & (
                    DestinationTokenValues.destination_vault_address
                    == AutopoolDestinationStates.destination_vault_address
                )
                & (DestinationTokenValues.block == AutopoolDestinationStates.block)
                & (DestinationTokenValues.chain_id == AutopoolDestinationStates.chain_id),
            ),
            TableSelector(
                table=DestinationStates,
                select_fields=[
                    DestinationStates.underlying_token_total_supply,
                    DestinationStates.lp_token_safe_price,
                    DestinationStates.incentive_apr,
                    DestinationStates.fee_apr,
                    DestinationStates.base_apr,
                    DestinationStates.fee_plus_base_apr,
                    DestinationStates.total_apr_out,
                    DestinationStates.total_apr_in,
                ],
                join_on=(DestinationStates.chain_id == DestinationTokenValues.chain_id)
                & (DestinationStates.destination_vault_address == DestinationTokenValues.destination_vault_address)
                & (DestinationStates.block == DestinationTokenValues.block),
            ),
            TableSelector(
                table=Destinations,
                select_fields=[Destinations.underlying_name, Destinations.exchange_name],
                join_on=(Destinations.chain_id == DestinationTokenValues.chain_id)
                & (Destinations.destination_vault_address == DestinationTokenValues.destination_vault_address),
            ),
            TableSelector(
                table=Blocks,
                select_fields=[Blocks.datetime],
                join_on=(Blocks.chain_id == TokenValues.chain_id) & (Blocks.block == TokenValues.block),
            ),
        ],
        where_clause=(AutopoolDestinationStates.autopool_vault_address == autopool.autopool_eth_addr)
        & (TokenValues.denominated_in == autopool.base_asset)
        & (DestinationTokenValues.denominated_in == autopool.base_asset)
        & (Tokens.chain_id == autopool.chain.chain_id)
        & (Blocks.datetime > autopool.get_display_date()),
    )

    destinations_df["readable_name"] = destinations_df.apply(
        lambda row: f"{row['underlying_name']} ({row['exchange_name']})", axis=1
    )

    # for the idle destination, owned shares == underlying_token_total_supply
    # not certain this is needed
    destinations_df.loc[
        destinations_df["destination_vault_address"] == autopool.autopool_eth_addr, "underlying_token_total_supply"
    ] = destinations_df.loc[destinations_df["destination_vault_address"] == autopool.autopool_eth_addr]["owned_shares"]

    # of the total supply, how much do we own,
    destinations_df["portion_owned"] = (
        destinations_df["owned_shares"] / destinations_df["underlying_token_total_supply"]
    )

    destinations_df["autopool_implied_safe_value"] = (
        destinations_df["portion_owned"] * destinations_df["quantity"] * destinations_df["safe_price"]
    )
    destinations_df["autopool_implied_backing_value"] = (
        destinations_df["portion_owned"] * destinations_df["quantity"] * destinations_df["backing"]
    )
    destinations_df["autopool_implied_quantity"] = destinations_df["portion_owned"] * destinations_df["quantity"]

    destinations_df["unweighted_expected_apr"] = 100 * destinations_df[
        ["fee_apr", "base_apr", "incentive_apr", "fee_plus_base_apr"]
    ].astype(float).fillna(0).sum(axis=1)

    return destinations_df


def get_readable_rebalance_events_by_autopool(autopool: AutopoolConstants) -> pd.DataFrame:
    rebalance_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                RebalanceEvents,
            ),
            TableSelector(
                Transactions, [Transactions.block], join_on=(Transactions.tx_hash == RebalanceEvents.tx_hash)
            ),
            TableSelector(
                AutopoolStates,
                [AutopoolStates.total_nav],
                join_on=(AutopoolStates.block == Transactions.block),
                row_filter=AutopoolStates.autopool_vault_address == autopool.autopool_eth_addr,
            ),
            TableSelector(
                RebalancePlans,
                [RebalancePlans.move_name],
                join_on=(RebalancePlans.file_name == RebalanceEvents.rebalance_file_path),
                row_filter=RebalancePlans.autopool_vault_address == autopool.autopool_eth_addr,
            ),
            TableSelector(
                Blocks,
                [Blocks.datetime],
                (Transactions.block == Blocks.block) & (Transactions.chain_id == Blocks.chain_id),
            ),
        ],
        where_clause=(RebalanceEvents.autopool_vault_address == autopool.autopool_eth_addr),
        order_by=Blocks.datetime,
    )

    tokens_df = merge_tables_as_df(
        [
            TableSelector(
                DestinationTokens,
            ),
            TableSelector(
                table=Tokens,
                select_fields=[Tokens.symbol, Tokens.decimals],
                join_on=(DestinationTokens.token_address == Tokens.token_address),
            ),
        ],
    )
    destinations_df = get_full_table_as_df(Destinations, where_clause=Destinations.chain_id == autopool.chain.chain_id)

    destination_to_underlying = destinations_df.set_index("destination_vault_address")["underlying_symbol"].to_dict()

    rebalance_df["destination_in_symbol"] = rebalance_df["destination_in"].map(destination_to_underlying)
    rebalance_df["destination_out_symbol"] = rebalance_df["destination_out"].map(destination_to_underlying)

    destination_token_address_to_symbols = (
        tokens_df.groupby("destination_vault_address")["symbol"].apply(tuple).apply(str).to_dict()
    )
    rebalance_df["destination_in_tokens"] = rebalance_df["destination_in"].map(destination_token_address_to_symbols)
    rebalance_df["destination_out_tokens"] = rebalance_df["destination_out"].map(destination_token_address_to_symbols)
    rebalance_df["tokens_move_name"] = (
        rebalance_df["destination_out_tokens"] + " -> " + rebalance_df["destination_in_tokens"]
    )
    return rebalance_df


def get_token_details_dict() -> tuple[dict, dict]:
    tokens_df = get_full_table_as_df(Tokens)  # not totally certain that token address is distinct across chains
    token_to_decimals = tokens_df.set_index(["token_address"])["decimals"].to_dict()
    token_to_symbol = tokens_df.set_index(["token_address"])["symbol"].to_dict()
    return token_to_decimals, token_to_symbol


def get_incentive_token_sold_details() -> pd.DataFrame:
    if st.session_state.get(SessionState.RECENT_START_DATE):
        where_clause = f"blocks.datetime >= '{st.session_state[SessionState.RECENT_START_DATE]}'"
    else:
        where_clause = "1=1"
    query = f"""SELECT
        incentive_token_swapped.tx_hash,
        incentive_token_swapped.log_index,
        incentive_token_swapped.chain_id,
        blocks.datetime,
        incentive_token_prices.third_party_price,
        incentive_token_swapped.sell_amount,
        incentive_token_swapped.buy_amount,
        incentive_token_swapped.buy_amount_received,
        sell_tokens.symbol AS sell,
        buy_tokens.symbol  AS buy,
        (incentive_token_swapped.buy_amount_received / NULLIF(incentive_token_swapped.sell_amount, 0)) AS actual_execution,
        (incentive_token_swapped.buy_amount / NULLIF(incentive_token_swapped.sell_amount, 0))          AS worst_possible_execution
        FROM incentive_token_swapped
        LEFT JOIN incentive_token_prices
        ON incentive_token_prices.tx_hash = incentive_token_swapped.tx_hash
        AND incentive_token_prices.log_index = incentive_token_swapped.log_index
        LEFT JOIN tokens AS sell_tokens
        ON sell_tokens.token_address = incentive_token_swapped.sell_token_address
        AND sell_tokens.chain_id      = incentive_token_swapped.chain_id
        LEFT JOIN tokens AS buy_tokens
        ON buy_tokens.token_address  = incentive_token_swapped.buy_token_address
        AND buy_tokens.chain_id       = incentive_token_swapped.chain_id
        JOIN transactions
        ON transactions.tx_hash = incentive_token_swapped.tx_hash
        JOIN blocks
        ON blocks.block = transactions.block
        AND blocks.chain_id = transactions.chain_id
        WHERE {where_clause}
    """
    df = _exec_sql_and_cache(query)
    return df


if __name__ == "__main__":
    for autopool in ALL_AUTOPOOLS:
        d = get_latest_rebalance_event_datetime_for_autopool(autopool)
        print(f"{autopool.name}\n  latest rebalance event datetime: {d}")
