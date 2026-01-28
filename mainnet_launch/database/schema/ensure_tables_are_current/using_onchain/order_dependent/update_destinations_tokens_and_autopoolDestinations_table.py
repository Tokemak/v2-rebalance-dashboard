import pandas as pd
from multicall import Call
from web3 import Web3


from mainnet_launch.constants import ChainData, ALL_CHAINS, ALL_AUTOPOOLS, DEAD_ADDRESS, PLASMA_CHAIN
from mainnet_launch.abis import AUTOPOOL_VAULT_ABI

from mainnet_launch.data_fetching.alchemy.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
    to_checksum_address_with_bool_success,
)
from mainnet_launch.database.schema.full import Destinations, DestinationTokens, Tokens, AutopoolDestinations
from mainnet_launch.database.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    get_full_table_as_df,
    bulk_overwrite,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    ensure_all_blocks_are_in_table,
)

# need this to not make redundant scans of plasma events
PHONY_DESTINATION_NAME = "phony_destination_to_not_double_scan_rows"


def ensure_all_tokens_are_saved_in_db(token_addresses: list[str], chain: ChainData) -> None:
    token_addresses = list(set([Web3.toChecksumAddress(addr) for addr in token_addresses]))
    token_addresses_to_add = get_subset_not_already_in_column(
        table=Tokens,
        column=Tokens.token_address,
        values=token_addresses,
        where_clause=(Tokens.chain_id == chain.chain_id),
    )

    if token_addresses_to_add:
        print(f"Adding {len(token_addresses_to_add)} tokens to Tokens table for chain {chain.name}")
        token_rows = _fetch_token_rows(token_addresses_to_add, chain)
        insert_avoid_conflicts(token_rows, Tokens)


def _fetch_token_rows(token_addresses: list[str], chain: ChainData) -> list[Tokens]:
    calls = []

    for t in token_addresses:
        calls.extend(
            [
                Call(t, "symbol()(string)", [((t, "symbol"), identity_with_bool_success)]),
                Call(t, "name()(string)", [((t, "name"), identity_with_bool_success)]),
                Call(t, "decimals()(uint256)", [((t, "decimals"), identity_with_bool_success)]),
            ]
        )

    raw = get_state_by_one_block(calls, block=chain.get_block_near_top(), chain=chain)

    return [
        Tokens(
            token_address=Web3.toChecksumAddress(t),
            chain_id=chain.chain_id,
            symbol=raw[(t, "symbol")],
            name=raw[(t, "name")],
            decimals=raw[(t, "decimals")],
        )
        for t in token_addresses
    ]


def _make_destination_vault_dicts(destination_vault_addreseses: list[str], chain: ChainData) -> list[dict]:
    calls = []
    for v in destination_vault_addreseses:
        calls.extend(
            [
                Call(v, "symbol()(string)", [((v, "symbol"), identity_with_bool_success)]),
                Call(v, "name()(string)", [((v, "name"), identity_with_bool_success)]),
                Call(v, "poolType()(string)", [((v, "pool_type"), identity_with_bool_success)]),
                Call(v, "exchangeName()(string)", [((v, "exchange_name"), identity_with_bool_success)]),
                Call(v, "underlying()(address)", [((v, "underlying"), to_checksum_address_with_bool_success)]),
                Call(v, "getPool()(address)", [((v, "pool"), to_checksum_address_with_bool_success)]),
                Call(v, "baseAsset()(address)", [((v, "base_asset"), to_checksum_address_with_bool_success)]),
                Call(v, "underlyingTokens()(address[])", [((v, "underlyingTokens"), identity_with_bool_success)]),
                Call(v, "decimals()(uint256)", [((v, "decimals"), identity_with_bool_success)]),
            ]
        )

    destination_vault_state = get_state_by_one_block(calls, block=chain.get_block_near_top(), chain=chain)

    under_calls = []
    for v in destination_vault_addreseses:
        underlying_addr = destination_vault_state[(v, "underlying")]
        under_calls.extend(
            [
                Call(underlying_addr, "symbol()(string)", [((v, "underlying_symbol"), identity_with_bool_success)]),
                Call(underlying_addr, "name()(string)", [((v, "underlying_name"), identity_with_bool_success)]),
            ]
        )

    tokens_raw = get_state_by_one_block(under_calls, block=chain.get_block_near_top(), chain=chain)
    destination_vault_state.update(tokens_raw)

    return destination_vault_state


def _make_idle_destinations(chain: ChainData) -> list[Destinations]:
    idle_details = []

    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == chain:
            idle_details.append(
                Destinations(
                    destination_vault_address=autopool.autopool_eth_addr,
                    chain_id=chain.chain_id,
                    exchange_name="tokemak",
                    name=autopool.name,
                    symbol=autopool.name,
                    pool_type="idle",
                    pool=autopool.autopool_eth_addr,
                    underlying=autopool.autopool_eth_addr,
                    underlying_symbol=autopool.name,
                    underlying_name=autopool.name,
                    denominated_in=autopool.base_asset,
                    destination_vault_decimals=18,  # always 18
                    block_deployed=autopool.block_deployed,
                )
            )

    return idle_details


def _make_idle_destination_tokens(chain: ChainData) -> list[DestinationTokens]:
    idle_details = []

    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == chain:
            idle_details.append(
                DestinationTokens(
                    destination_vault_address=autopool.autopool_eth_addr,
                    chain_id=chain.chain_id,
                    token_address=autopool.base_asset,
                    index=0,
                )
            )

    return idle_details


def _make_idle_autopool_destinations(chain: ChainData) -> list[AutopoolDestinations]:
    idle_details = []

    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == chain:
            idle_details.append(
                AutopoolDestinations(
                    destination_vault_address=autopool.autopool_eth_addr,
                    chain_id=chain.chain_id,
                    autopool_vault_address=autopool.autopool_eth_addr,
                )
            )

    return idle_details


def _build_destination_rows(
    chain: ChainData, DestinationVaultAdded: pd.DataFrame, destination_vault_state: dict
) -> tuple[list[Destinations], list[AutopoolDestinations], list[DestinationTokens]]:
    """
    Builds the Destinations, AutopoolDestinations, and DestinationTokens rows to be added to the database
    always include idle destinations as well
    """
    destination_vault_address_to_block_deployed = DestinationVaultAdded.set_index("destination")["block"].to_dict()

    all_destinations: list[Destinations] = _make_idle_destinations(chain)
    all_autopool_destinations: list[AutopoolDestinations] = _make_idle_autopool_destinations(chain)
    all_destination_tokens: list[DestinationTokens] = _make_idle_destination_tokens(chain)

    for destination_vault_address, autopool_vault_address in zip(
        DestinationVaultAdded["destination"], DestinationVaultAdded["autopool"]
    ):
        new_destination_row = Destinations(
            destination_vault_address=destination_vault_address,
            chain_id=chain.chain_id,
            exchange_name=destination_vault_state[(destination_vault_address, "exchange_name")],
            name=destination_vault_state[(destination_vault_address, "name")],
            symbol=destination_vault_state[(destination_vault_address, "symbol")],
            pool_type=destination_vault_state[(destination_vault_address, "pool_type")],
            pool=destination_vault_state[(destination_vault_address, "pool")],
            underlying=destination_vault_state[(destination_vault_address, "underlying")],
            underlying_symbol=destination_vault_state[(destination_vault_address, "underlying_symbol")],
            underlying_name=destination_vault_state[(destination_vault_address, "underlying_name")],
            denominated_in=destination_vault_state[(destination_vault_address, "base_asset")],
            destination_vault_decimals=(destination_vault_state[(destination_vault_address, "decimals")]),
            block_deployed=destination_vault_address_to_block_deployed[destination_vault_address],
        )

        all_destinations.append(new_destination_row)

        all_autopool_destinations.append(
            AutopoolDestinations(
                destination_vault_address=destination_vault_address,
                chain_id=chain.chain_id,
                autopool_vault_address=autopool_vault_address,
            )
        )

        for index, token_address in enumerate(destination_vault_state[(destination_vault_address, "underlyingTokens")]):
            all_destination_tokens.append(
                DestinationTokens(
                    destination_vault_address=destination_vault_address,
                    chain_id=chain.chain_id,
                    token_address=Web3.toChecksumAddress(token_address),
                    index=index,
                )
            )

    return all_destinations, all_autopool_destinations, all_destination_tokens


def overwrite_phony_destination_block_scanned_to_row(chain: ChainData, block_scanned_to: int) -> None:
    """Just useful for not scanning too many plasma events every time"""

    phony_row = Destinations(
        destination_vault_address=DEAD_ADDRESS,
        chain_id=chain.chain_id,
        exchange_name="tokemak",
        name=PHONY_DESTINATION_NAME,
        symbol="phony",
        pool_type="idle",
        pool=DEAD_ADDRESS,
        underlying=DEAD_ADDRESS,
        underlying_symbol="phony",
        underlying_name="phony",
        denominated_in=DEAD_ADDRESS,
        destination_vault_decimals=18,
        block_deployed=block_scanned_to,
    )

    bulk_overwrite((phony_row,), Destinations)


def ensure__destinations__tokens__and__destination_tokens_are_current() -> None:
    """
    Make sure that the Destinations, DestinationTokens and Tokens tables are current for all the underlying tokens in each of the destinations
    """
    current_destinations = get_full_table_as_df(Destinations)

    chain_to_highest_block_seen = current_destinations.groupby("chain_id")["block_deployed"].max().to_dict()
    for c in ALL_CHAINS:
        if c.chain_id not in chain_to_highest_block_seen or pd.isna(chain_to_highest_block_seen[c.chain_id]):
            chain_to_highest_block_seen[c.chain_id] = (
                c.block_autopool_first_deployed
            )  # not certain these are convervative enough

    for chain in ALL_CHAINS:
        top_block = chain.get_block_near_top()
        print(f"Ensuring Destinations, DestinationTokens, and Tokens are current for chain {chain.name}")
        autopools = [a for a in ALL_AUTOPOOLS if a.chain == chain]

        autopool_vault_contract = chain.client.eth.contract(autopools[0].autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
        # this part make redundant calls,
        DestinationVaultAdded = fetch_events(
            autopool_vault_contract.events.DestinationVaultAdded,
            chain=chain,
            start_block=chain_to_highest_block_seen[chain.chain_id] + 1,
            end_block=top_block,
            addresses=[a.autopool_eth_addr for a in autopools],
        )

        DestinationVaultAdded["autopool"] = DestinationVaultAdded["address"]
        DestinationVaultAdded["destination"] = DestinationVaultAdded["destination"].apply(
            lambda x: Web3.toChecksumAddress(x)
        )

        destination_vault_state = _make_destination_vault_dicts(
            [v for v in DestinationVaultAdded["destination"]], chain
        )

        all_destinations, all_autopool_destinations, all_destination_tokens = _build_destination_rows(
            chain, DestinationVaultAdded, destination_vault_state
        )

        blocks = [top_block, *[d.block_deployed for d in all_destinations]]

        ensure_all_blocks_are_in_table(blocks, chain)

        insert_avoid_conflicts(
            all_destinations,
            Destinations,
        )

        tokens_expected_to_have = [d.token_address for d in all_destination_tokens] + [
            d.underlying for d in all_destinations
        ]
        ensure_all_tokens_are_saved_in_db(tokens_expected_to_have, chain)
        insert_avoid_conflicts(
            all_destination_tokens,
            DestinationTokens,
        )

        insert_avoid_conflicts(all_autopool_destinations, AutopoolDestinations)
        print("successfully updated tables for chain", chain.name)

        # overwrite_phony_destination_block_scanned_to_row(chain, top_block)


if __name__ == "__main__":
    # ensure__destinations__tokens__and__destination_tokens_are_current()

    from mainnet_launch.constants import ETH_CHAIN, profile_function

    profile_function(ensure__destinations__tokens__and__destination_tokens_are_current)


# def ensure__destinations__tokens__and__destination_tokens_are_current_old() -> None:
#     """
#     Make sure that the Destinations, DestinationTokens and Tokens tables are current for all the underlying tokens in each of the destinations
#     """
#     for chain in ALL_CHAINS:
#         autopool_vault_added_dfs = []

#         for autopool in ALL_AUTOPOOLS:
#             if autopool.chain != chain:
#                 continue

#             autopool_vault_contract = chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)
#             # this part make redundant calls,
#             DestinationVaultAdded = fetch_events(
#                 autopool_vault_contract.events.DestinationVaultAdded,
#                 chain=chain,
#                 start_block=autopool.block_deployed,
#             )
#             DestinationVaultAdded["autopool"] = autopool.autopool_eth_addr
#             DestinationVaultAdded["destination"] = DestinationVaultAdded["destination"].apply(
#                 lambda x: Web3.toChecksumAddress(x)
#             )

#             autopool_vault_added_dfs.append(DestinationVaultAdded)

#         DestinationVaultAdded = pd.concat(autopool_vault_added_dfs, axis=0)

#         destination_vault_state = _make_destination_vault_dicts(
#             [v for v in DestinationVaultAdded["destination"]], chain
#         )

#         new_autopool_destinations = _make_idle_autopool_destinations(chain)

#         new_destination_rows = []
#         destination_tokens = []
#         for v, autopool_vault_address in zip(DestinationVaultAdded["destination"], DestinationVaultAdded["autopool"]):
#             new_destination_row = Destinations(
#                 destination_vault_address=v,
#                 chain_id=chain.chain_id,
#                 exchange_name=destination_vault_state[(v, "exchange_name")],
#                 name=destination_vault_state[(v, "name")],
#                 symbol=destination_vault_state[(v, "symbol")],
#                 pool_type=destination_vault_state[(v, "pool_type")],
#                 pool=destination_vault_state[(v, "pool")],
#                 underlying=destination_vault_state[(v, "underlying")],
#                 underlying_symbol=destination_vault_state[(v, "underlying_symbol")],
#                 underlying_name=destination_vault_state[(v, "underlying_name")],
#                 denominated_in=destination_vault_state[(v, "base_asset")],
#                 destination_vault_decimals=(destination_vault_state[(v, "decimals")]),
#             )

#             new_destination_rows.append(new_destination_row)

#             new_autopool_destinations.append(
#                 AutopoolDestinations(
#                     destination_vault_address=v,
#                     chain_id=chain.chain_id,
#                     autopool_vault_address=autopool_vault_address,
#                 )
#             )

#             for index, token_address in enumerate(destination_vault_state[(v, "underlyingTokens")]):
#                 destination_tokens.append(
#                     DestinationTokens(
#                         destination_vault_address=v,
#                         chain_id=chain.chain_id,
#                         token_address=Web3.toChecksumAddress(token_address),
#                         index=index,
#                     )
#                 )

#         idle_destinations = _make_idle_destinations(chain)

#         insert_avoid_conflicts(
#             [*new_destination_rows, *idle_destinations],
#             Destinations,
#             index_elements=[Destinations.destination_vault_address, Destinations.chain_id],
#         )

#         base_asset_tokens_to_get = [
#             base_asset(chain) for base_asset in ALL_BASE_ASSETS if base_asset(chain) != DEAD_ADDRESS
#         ]

#         tokens_addresses_to_get = [
#             *[t.token_address for t in destination_tokens],
#             *[t.underlying for t in new_destination_rows],
#             *base_asset_tokens_to_get,
#         ]
#         tokens_addresses_to_get = list(set(tokens_addresses_to_get))
#         new_token_rows = _fetch_token_rows(tokens_addresses_to_get, chain)

#         insert_avoid_conflicts(
#             new_token_rows,
#             Tokens,
#             index_elements=[Tokens.token_address, Tokens.chain_id],
#         )

#         idle_destination_tokens = _make_idle_destination_tokens(chain)

#         insert_avoid_conflicts(
#             [*destination_tokens, *idle_destination_tokens],
#             DestinationTokens,
#             index_elements=[
#                 DestinationTokens.destination_vault_address,
#                 DestinationTokens.chain_id,
#                 DestinationTokens.token_address,
#             ],
#         )

#         insert_avoid_conflicts(
#             new_autopool_destinations,
#             AutopoolDestinations,
#             index_elements=[
#                 AutopoolDestinations.autopool_vault_address,
#                 AutopoolDestinations.chain_id,
#                 AutopoolDestinations.destination_vault_address,
#             ],
#         )
