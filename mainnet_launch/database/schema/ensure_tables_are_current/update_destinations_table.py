import pandas as pd
from multicall import Call
from web3 import Web3


from mainnet_launch.constants import DESTINATION_VAULT_REGISTRY, ChainData, ALL_CHAINS, ALL_AUTOPOOLS
from mainnet_launch.abis import DESTINATION_VAULT_REGISTRY_ABI

from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.data_fetching.get_state_by_block import get_state_by_one_block, identity_with_bool_success
from mainnet_launch.data_fetching.block_timestamp import ensure_all_blocks_are_in_table

from mainnet_launch.database.schema.full import Destinations, DestinationTokens, Tokens
from mainnet_launch.database.schema.postgres_operations import insert_avoid_conflicts, get_highest_value_in_field_where


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


def _make_destination_vault_dicts(DestinationVaultRegistered: pd.DataFrame, chain: ChainData) -> list[dict]:
    highest_block = int(DestinationVaultRegistered["block"].max())
    vaults = list(DestinationVaultRegistered["vaultAddress"])
    # 1) build all the on-chain calls with tuple keys
    calls = []
    for v in vaults:
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
            ]
        )

    destination_vault_state = get_state_by_one_block(calls, block=highest_block, chain=chain)

    under_calls = []
    for v in vaults:
        underlying_addr = destination_vault_state[(v, "underlying")]
        under_calls.extend(
            [
                Call(underlying_addr, "symbol()(string)", [((v, "underlying_symbol"), identity_with_bool_success)]),
                Call(underlying_addr, "name()(string)", [((v, "underlying_name"), identity_with_bool_success)]),
            ]
        )

    tokens_raw = get_state_by_one_block(under_calls, block=highest_block, chain=chain)

    # 3) map vault → block for the return data
    vault_to_block = {
        (v, "block_deployed"): b
        for v, b in zip(DestinationVaultRegistered["vaultAddress"], DestinationVaultRegistered["block"])
    }

    destination_vault_state.update(tokens_raw)
    destination_vault_state.update(vault_to_block)
    return destination_vault_state


def _make_destination_vault_dicts2(DestinationVaultRegistered: pd.DataFrame, chain: ChainData):
    highest_block = int(DestinationVaultRegistered["block"].max())
    vaults = DestinationVaultRegistered["vaultAddress"]

    symbol_calls = [
        Call(
            v,
            "symbol()(string)",
            [(v + "_symbol", identity_with_bool_success)],
        )
        for v in vaults
    ]

    name_calls = [
        Call(
            v,
            "name()(string)",
            [(v + "_name", identity_with_bool_success)],
        )
        for v in vaults
    ]

    pool_type_calls = [
        Call(
            v,
            "poolType()(string)",
            [(v + "_pool_type", identity_with_bool_success)],
        )
        for v in vaults
    ]

    exchange_name_calls = [
        Call(
            v,
            "exchangeName()(string)",
            [(v + "_exchange_name", identity_with_bool_success)],
        )
        for v in vaults
    ]

    underlying_calls = [
        Call(
            v,
            "underlying()(address)",
            [(v + "_underlying", identity_with_bool_success)],
        )
        for v in vaults
    ]

    pool_calls = [
        Call(
            v,
            "getPool()(address)",
            [(v + "_pool", identity_with_bool_success)],
        )
        for v in vaults
    ]

    base_asset_calls = [
        Call(
            v,
            "baseAsset()(address)",
            [(v + "_base_asset", identity_with_bool_success)],
        )
        for v in vaults
    ]

    underlying_tokens_calls = [
        Call(
            v,
            "underlyingTokens()(address[])",
            [(v + "_underlyingTokens", identity_with_bool_success)],
        )
        for v in vaults
    ]
    calls = [
        *symbol_calls,
        *name_calls,
        *pool_type_calls,
        *exchange_name_calls,
        *underlying_calls,
        *pool_calls,
        *base_asset_calls,
        *underlying_tokens_calls,
    ]

    raw = get_state_by_one_block(calls, highest_block, chain)
    # vaults = DestinationVaultRegistered["vaultAddress"]

    # one dict per attribute
    symbol_dict = {v: raw[f"{v}_symbol"] for v in vaults}
    name_dict = {v: raw[f"{v}_name"] for v in vaults}
    pool_type_dict = {v: raw[f"{v}_pool_type"] for v in vaults}
    exchange_name_dict = {v: raw[f"{v}_exchange_name"] for v in vaults}
    underlying_dict = {v: raw[f"{v}_underlying"] for v in vaults}
    pool_dict = {v: raw[f"{v}_pool"] for v in vaults}
    base_asset_dict = {v: raw[f"{v}_base_asset"] for v in vaults}
    underlying_tokens_dict = {v: raw[f"{v}_underlyingTokens"] for v in vaults}

    underlying_symbol_calls = [
        Call(
            underlying_dict[v],
            "symbol()(string)",
            [(v + "_underlying_symbol", identity_with_bool_success)],
        )
        for v in vaults
    ]

    underlying_names_calls = [
        Call(
            underlying_dict[v],
            "symbol()(string)",
            [(v + "_underlying_name", identity_with_bool_success)],
        )
        for v in vaults
    ]

    tokens_raw = get_state_by_one_block([*underlying_symbol_calls, *underlying_names_calls], highest_block, chain)

    underlying_symbol_dict = {v: tokens_raw[f"{v}_underlying_symbol"] for v in vaults}
    underlying_name_dict = {v: tokens_raw[f"{v}_underlying_name"] for v in vaults}
    vault_to_block = {
        v: b for v, b in zip(DestinationVaultRegistered["vaultAddress"], DestinationVaultRegistered["block"])
    }

    return (
        symbol_dict,
        name_dict,
        pool_type_dict,
        exchange_name_dict,
        underlying_dict,
        pool_dict,
        base_asset_dict,
        underlying_tokens_dict,
        underlying_symbol_dict,
        underlying_name_dict,
        vault_to_block,
    )


def _make_idle_destinations(chain: ChainData) -> list[Destinations]:
    idle_details = []

    for autopool in ALL_AUTOPOOLS:
        if autopool.chain == chain:
            idle_details.append(
                Destinations(
                    destination_vault_address=Web3.toChecksumAddress(autopool.autopool_eth_addr),
                    chain_id=chain.chain_id,
                    exchange_name="tokemak",
                    block_deployed=autopool.block_deployed,
                    name=autopool.name,
                    symbol=autopool.name,
                    pool_type="idle",
                    pool=autopool.autopool_eth_addr,
                    underlying=autopool.autopool_eth_addr,
                    underlying_symbol=autopool.name,
                    underlying_name=autopool.name,
                    denominated_in=autopool.base_asset,
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
        highest_block_already_found = get_highest_value_in_field_where(
            Destinations, Destinations.block_deployed, where_clause=Destinations.chain_id == chain.chain_id
        )
        highest_block_already_found = (
            chain.block_autopool_first_deployed if highest_block_already_found is None else highest_block_already_found
        )
        contract = chain.client.eth.contract(DESTINATION_VAULT_REGISTRY(chain), abi=DESTINATION_VAULT_REGISTRY_ABI)

        DestinationVaultRegistered = fetch_events(
            contract.events.DestinationVaultRegistered,
            start_block=highest_block_already_found + 1,  # +1 avoids fetching the last event again
            end_block=chain.client.eth.block_number,
            chain=chain,
        )
        if len(DestinationVaultRegistered) == 0:
            # early stop if no vaults
            continue

        destination_vault_state = _make_destination_vault_dicts(DestinationVaultRegistered, chain)

        destinations = [
            Destinations(
                destination_vault_address=Web3.toChecksumAddress(v),
                chain_id=chain.chain_id,
                exchange_name=destination_vault_state[(v, "exchange_name")],
                block_deployed=destination_vault_state[(v, "block_deployed")],
                name=destination_vault_state[(v, "name")],
                symbol=destination_vault_state[(v, "symbol")],
                pool_type=destination_vault_state[(v, "pool_type")],
                pool=Web3.toChecksumAddress(destination_vault_state[(v, "pool")]),
                underlying=Web3.toChecksumAddress(destination_vault_state[(v, "underlying")]),
                underlying_symbol=destination_vault_state[(v, "underlying_symbol")],
                underlying_name=destination_vault_state[(v, "underlying_name")],
                denominated_in=destination_vault_state[(v, "base_asset")],
            )
            for v in DestinationVaultRegistered["vaultAddress"]
        ]

        destination_tokens = []
        for v in DestinationVaultRegistered["vaultAddress"]:
            for index, token_address in enumerate(destination_vault_state[(v, "underlyingTokens")]):
                destination_tokens.append(
                    DestinationTokens(
                        destination_vault_address=Web3.toChecksumAddress(v),
                        chain_id=chain.chain_id,
                        token_address=Web3.toChecksumAddress(token_address),
                        index=index,
                    )
                )

        tokens = _fetch_token_rows(set([t.token_address for t in destination_tokens]), chain)
        blocks_to_make_sure_exist = [d.block_deployed for d in destinations]

        ensure_all_blocks_are_in_table(blocks_to_make_sure_exist, chain)

        insert_avoid_conflicts(tokens, Tokens, index_elements=[Tokens.token_address, Tokens.chain_id])

        insert_avoid_conflicts(
            destinations, Destinations, index_elements=[Destinations.destination_vault_address, Destinations.chain_id]
        )

        idle_destinations = _make_idle_destinations(chain)

        insert_avoid_conflicts(
            idle_destinations,
            Destinations,
            index_elements=[Destinations.destination_vault_address, Destinations.chain_id],
        )

        insert_avoid_conflicts(
            destination_tokens,
            DestinationTokens,
            index_elements=[
                DestinationTokens.destination_vault_address,
                DestinationTokens.chain_id,
                DestinationTokens.token_address,
            ],
        )

        idle_destination_tokens = _make_idle_destination_tokens(chain)

        insert_avoid_conflicts(
            idle_destination_tokens,
            DestinationTokens,
            index_elements=[
                DestinationTokens.destination_vault_address,
                DestinationTokens.chain_id,
                DestinationTokens.token_address,
            ],
        )


if __name__ == "__main__":
    ensure_destinations__tokens__and__destination_tokens_are_current()


# def fetch_all_destinations_for_autopool(autopool: AutopoolConstants) -> list[Destinations]:
#     """
#     Retrieves all destination rows associated with a specific autopool.

#     :param autopool: AutopoolConstants enum to filter by.
#     :return: List of Destinations ORM instances.
#     """
#     with Session.begin() as session:
#         destinations = (
#             session.execute(select(Destinations).where(Destinations.autopool == autopool.autopool_eth_addr))
#             .scalars()
#             .all()
#         )
#     return destinations


# # DestinationVaultFactory


# def _fetch_destinations_from_external_source(
#     chain: ChainData, highest_block_already_fetched: int
# ) -> list[Destinations]:
#     blocks = build_blocks_to_use(chain, start_block=highest_block_already_fetched)

#     # could we do this clearer? from the events? yes (update later)

#     # returns a list of all destinations along with their autopools even if the destinations have been replaced
#     pools_and_destinations_df = fetch_pools_and_destinations_df(chain, blocks)
#     autopool_pool_address_to_autopool = {a.autopool_eth_addr.lower(): a for a in ALL_AUTOPOOLS}

#     all_destination_details: list[Destinations] = make_idle_destination_details(chain)

#     def _add_to_all_destination_details(row):
#         for on_chain_autopool_data, list_of_destinations in zip(
#             row["getPoolsAndDestinations"]["autopools"], row["getPoolsAndDestinations"]["destinations"]
#         ):
#             autopool_constant = autopool_pool_address_to_autopool.get(on_chain_autopool_data["poolAddress"].lower())
#             # skip autopools that don't have an AutopoolConstant setup
#             # this is so that the app won't break when a new autopool is deployed
#             if autopool_constant is not None:
#                 for destination in list_of_destinations:
#                     destination_details = Destinations(
#                         vaultAddress=Web3.toChecksumAddress(destination["vaultAddress"]),
#                         exchangeName=destination["exchangeName"],
#                         dexPool=Web3.toChecksumAddress(destination["dexPool"]),
#                         lpTokenAddress=Web3.toChecksumAddress(destination["lpTokenAddress"]),
#                         lpTokenName=destination["lpTokenName"],
#                         lpTokenSymbol=destination["lpTokenSymbol"],
#                         autopool=autopool_constant,
#                         vault_name=None,  # added later with an onchain call
#                     )
#                     # add any destinations ever created regardless of if they are currently active
#                     all_destination_details.append(destination_details)

#     pools_and_destinations_df.apply(_add_to_all_destination_details, axis=1)

#     unique_destination_vault_addressses = set([dest.vaultAddress for dest in all_destination_details])


# def _fetch_destination_details_from_external_source2(
#     chain: ChainData, highest_block_already_fetched: int
# ) -> pd.DataFrame:

#     blocks = build_blocks_to_use(chain, start_block=highest_block_already_fetched)
#     # returns a list of all destinations along with their autopools even if the destinations have been replaced
#     pools_and_destinations_df = fetch_pools_and_destinations_df(chain, blocks)
#     autopool_pool_address_to_autopool = {a.autopool_eth_addr.lower(): a for a in ALL_AUTOPOOLS}

#     if highest_block_already_fetched == chain.block_autopool_first_deployed:
#         all_destination_details: list[DestinationDetails] = make_idle_destination_details(chain)
#     else:
#         all_destination_details = []

#     def _add_to_all_destination_details(row):
#         for on_chain_autopool_data, list_of_destinations in zip(
#             row["getPoolsAndDestinations"]["autopools"], row["getPoolsAndDestinations"]["destinations"]
#         ):
#             autopool_constant = autopool_pool_address_to_autopool.get(on_chain_autopool_data["poolAddress"].lower())
#             # skip autopools that don't have an AutopoolConstant setup
#             # this is so that the app won't break when a new autopool is deployed
#             if autopool_constant is not None:
#                 for destination in list_of_destinations:
#                     destination_details = DestinationDetails(
#                         vaultAddress=Web3.toChecksumAddress(destination["vaultAddress"]),
#                         exchangeName=destination["exchangeName"],
#                         dexPool=Web3.toChecksumAddress(destination["dexPool"]),
#                         lpTokenAddress=Web3.toChecksumAddress(destination["lpTokenAddress"]),
#                         lpTokenName=destination["lpTokenName"],
#                         lpTokenSymbol=destination["lpTokenSymbol"],
#                         autopool=autopool_constant,
#                         vault_name=None,  # added later with an onchain call
#                     )
#                     # add any destinations ever created regardless of if they are currently active
#                     all_destination_details.append(destination_details)

#     pools_and_destinations_df.apply(_add_to_all_destination_details, axis=1)

#     unique_destination_vault_addressses = set([dest.vaultAddress for dest in all_destination_details])

#     get_destination_names_calls = [
#         Call(
#             vaultAddress,
#             "symbol()(string)",
#             [(Web3.toChecksumAddress(vaultAddress), identity_with_bool_success)],
#         )
#         for vaultAddress in unique_destination_vault_addressses
#     ]
#     # the names don't change so we only need to get it once at the current highest block
#     vault_addresses_to_names = get_state_by_one_block(get_destination_names_calls, block=max(blocks), chain=chain)

#     for dest in all_destination_details:
#         symbol = vault_addresses_to_names[Web3.toChecksumAddress(dest.vaultAddress)]
#         symbol = symbol.replace("toke-WETH-", "")
#         dest.vault_name = f"{symbol} ({dest.exchangeName})"

#     destination_details_df = pd.DataFrame.from_records([dest.to_record() for dest in all_destination_details])

#     destination_details_df = destination_details_df.drop_duplicates(keep="first")
#     return destination_details_df, max(blocks)


# # def add_new_destination_details_for_each_chain_to_table():
# #     for chain in ALL_CHAINS:
# #         highest_block_already_fetched = get_earliest_block_from_table_with_chain(CHAIN_BLOCK_QUERIED_TABLE, chain)
# #         new_destination_details_df, new_highest_block = _fetch_destination_details_from_external_source(
# #             chain, highest_block_already_fetched
# #         )
# #         chain_block_table = pd.DataFrame.from_records([{"block": new_highest_block, "chain": chain.name}])
# #         write_dataframe_to_table(chain_block_table, CHAIN_BLOCK_QUERIED_TABLE)
# #         write_dataframe_to_table(new_destination_details_df, DESTINATION_DETAILS_TABLE)
