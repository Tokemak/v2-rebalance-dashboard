import pandas as pd
from multicall import Call
from web3 import Web3


from mainnet_launch.constants import DESTINATION_VAULT_REGISTRY, ChainData, ALL_CHAINS, ALL_AUTOPOOLS
from mainnet_launch.abis import DESTINATION_VAULT_REGISTRY_ABI, AUTOPOOL_VAULT_ABI

from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import (
    get_state_by_one_block,
    identity_with_bool_success,
    build_blocks_to_use,
)
from mainnet_launch.data_fetching.block_timestamp import ensure_all_blocks_are_in_table

from mainnet_launch.database.schema.full import Destinations, DestinationTokens, Tokens, AutopoolDestinations
from mainnet_launch.database.schema.postgres_operations import insert_avoid_conflicts, get_highest_value_in_field_where

from mainnet_launch.pages.autopool_diagnostics.lens_contract import (
    fetch_autopool_to_active_destinations_over_this_period_of_missing_blocks_address,
)


def _fetch_token_rows(token_addresses: list[str], chain: ChainData) -> list[Tokens]:
    symbol_calls = [
        Call(
            t,
            "symbol()(string)",
            [((t, "symbol"), identity_with_bool_success)],
        )
        for t in token_addresses
    ]

    name_calls = [
        Call(
            t,
            "name()(string)",
            [((t, "name"), identity_with_bool_success)],
        )
        for t in token_addresses
    ]

    decimals_calls = [
        Call(
            t,
            "decimals()(uint256)",
            [((t, "decimals"), identity_with_bool_success)],
        )
        for t in token_addresses
    ]

    raw = get_state_by_one_block(
        [*symbol_calls, *name_calls, *decimals_calls], block=chain.client.eth.block_number, chain=chain
    )

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


def _make_destination_vault_dicts(
    destination_vault_addreseses: list[str], highest_block: int, chain: ChainData
) -> list[dict]:
    # 1) build all the on-chain calls with tuple keys
    calls = []
    for v in destination_vault_addreseses:
        calls.extend(
            [
                Call(v, "symbol()(string)", [((v, "symbol"), identity_with_bool_success)]),
                Call(v, "name()(string)", [((v, "name"), identity_with_bool_success)]),
                Call(v, "poolType()(string)", [((v, "pool_type"), identity_with_bool_success)]),
                Call(v, "exchangeName()(string)", [((v, "exchange_name"), identity_with_bool_success)]),
                Call(v, "underlying()(address)", [((v, "underlying"), identity_with_bool_success)]),
                Call(v, "getPool()(address)", [((v, "pool"), identity_with_bool_success)]),
                Call(v, "baseAsset()(address)", [((v, "base_asset"), identity_with_bool_success)]),
                Call(v, "underlyingTokens()(address[])", [((v, "underlyingTokens"), identity_with_bool_success)]),
                Call(v, "decimals()(uint256)", [((v, "decimals"), identity_with_bool_success)]),
            ]
        )

    destination_vault_state = get_state_by_one_block(calls, block=highest_block, chain=chain)

    under_calls = []
    for v in destination_vault_addreseses:
        underlying_addr = destination_vault_state[(v, "underlying")]
        under_calls.extend(
            [
                Call(underlying_addr, "symbol()(string)", [((v, "underlying_symbol"), identity_with_bool_success)]),
                Call(underlying_addr, "name()(string)", [((v, "underlying_name"), identity_with_bool_success)]),
            ]
        )

    tokens_raw = get_state_by_one_block(under_calls, block=highest_block, chain=chain)

    destination_vault_state.update(tokens_raw)
    return destination_vault_state


def _make_idle_destinations(chain: ChainData) -> list[Destinations]:
    idle_details = []

    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == chain:
            idle_details.append(
                Destinations(
                    destination_vault_address=Web3.toChecksumAddress(autopool.autopool_eth_addr),
                    chain_id=chain.chain_id,
                    exchange_name="tokemak",
                    name=autopool.name,
                    symbol=autopool.name,
                    pool_type="idle",
                    pool=autopool.autopool_eth_addr,
                    underlying=autopool.autopool_eth_addr,
                    underlying_symbol=autopool.name,
                    underlying_name=autopool.name,
                    denominated_in=Web3.toChecksumAddress(autopool.base_asset),
                    destination_vault_decimals=18,  # always 18
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


def ensure__destinations__tokens__and__destination_tokens_are_current() -> None:
    """
    Make sure that the Destinations, DestinationTokens and Tokens tables are current for all the underlying tokens in each of the destinations
    """
    for chain in ALL_CHAINS:
        autopool_vault_added_dfs = []

        for autopool in ALL_AUTOPOOLS:
            if autopool.chain != chain:
                continue
            autopool_vault_contract = chain.client.eth.contract(autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_ABI)

            DestinationVaultAdded = fetch_events(
                autopool_vault_contract.events.DestinationVaultAdded,
                start_block=autopool.block_deployed,
                chain=chain,
            )
            DestinationVaultAdded["autopool"] = autopool.autopool_eth_addr
            DestinationVaultAdded["destination"] = DestinationVaultAdded["destination"].apply(
                lambda x: Web3.toChecksumAddress(x)
            )

            autopool_vault_added_dfs.append(DestinationVaultAdded)

        DestinationVaultAdded = pd.concat(autopool_vault_added_dfs, axis=0)

        destination_vault_state = _make_destination_vault_dicts(
            [v for v in DestinationVaultAdded["destination"]], max(DestinationVaultAdded["block"].astype(int)), chain
        )

        new_autopool_destinations = []

        new_destination_rows = []
        for v, autopool_vault_address in zip(DestinationVaultAdded["destination"], DestinationVaultAdded["autopool"]):
            new_destination_row = Destinations(
                destination_vault_address=Web3.toChecksumAddress(v),
                chain_id=chain.chain_id,
                exchange_name=destination_vault_state[(v, "exchange_name")],
                name=destination_vault_state[(v, "name")],
                symbol=destination_vault_state[(v, "symbol")],
                pool_type=destination_vault_state[(v, "pool_type")],
                pool=Web3.toChecksumAddress(destination_vault_state[(v, "pool")]),
                underlying=Web3.toChecksumAddress(destination_vault_state[(v, "underlying")]),
                underlying_symbol=destination_vault_state[(v, "underlying_symbol")],
                underlying_name=destination_vault_state[(v, "underlying_name")],
                denominated_in=Web3.toChecksumAddress(destination_vault_state[(v, "base_asset")]),
                destination_vault_decimals=(destination_vault_state[(v, "decimals")]),
            )

            new_destination_rows.append(new_destination_row)

            new_autopool_destinations.append(
                AutopoolDestinations(
                    destination_vault_address=Web3.toChecksumAddress(v),
                    chain_id=chain.chain_id,
                    autopool_vault_address=autopool_vault_address,
                )
            )

        destination_tokens = []
        for dest in new_destination_rows:
            v = dest.destination_vault_address
            for index, token_address in enumerate(destination_vault_state[(v, "underlyingTokens")]):
                destination_tokens.append(
                    DestinationTokens(
                        destination_vault_address=Web3.toChecksumAddress(v),
                        chain_id=chain.chain_id,
                        token_address=Web3.toChecksumAddress(token_address),
                        index=index,
                    )
                )

        idle_destinations = _make_idle_destinations(chain)

        insert_avoid_conflicts(
            [*new_destination_rows, *idle_destinations],
            Destinations,
            index_elements=[Destinations.destination_vault_address, Destinations.chain_id],
        )

        tokens = _fetch_token_rows(set([t.token_address for t in destination_tokens]), chain)
        underlying_tokens = _fetch_token_rows(set([t.underlying for t in new_destination_rows]), chain)

        insert_avoid_conflicts(
            [*tokens, *underlying_tokens], Tokens, index_elements=[Tokens.token_address, Tokens.chain_id]
        )

        idle_destination_tokens = _make_idle_destination_tokens(chain)

        insert_avoid_conflicts(
            [*destination_tokens, *idle_destination_tokens],
            DestinationTokens,
            index_elements=[
                DestinationTokens.destination_vault_address,
                DestinationTokens.chain_id,
                DestinationTokens.token_address,
            ],
        )

        insert_avoid_conflicts(
            new_autopool_destinations,
            AutopoolDestinations,
            index_elements=[
                AutopoolDestinations.autopool_vault_address,
                AutopoolDestinations.chain_id,
                AutopoolDestinations.destination_vault_address,
            ],
        )


if __name__ == "__main__":
    ensure__destinations__tokens__and__destination_tokens_are_current()
