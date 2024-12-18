import pandas as pd
import streamlit as st

from multicall import Call
from v2_rebalance_dashboard.get_state_by_block import (
    sync_safe_get_raw_state_by_block,
    sync_get_raw_state_by_block_one_block,
    build_blocks_to_use,
    safe_normalize_with_bool_success,
    identity_with_bool_success,
    identity_function,
)
import plotly.express as px
from v2_rebalance_dashboard.constants import (
    eth_client,
    BALANCER_VAULT_ADDRESS,
    ROOT_PRICE_ORACLE,
    balETH_AUTOPOOL_ETH_ADDRESS,
    ROOT_DIR,
)


# { # for balancer
#         address token = reserveTokens[index];

#         // the price oracle is always 18 decimals, so divide by the decimals of the token
#         // to ensure that we always report the value in ETH as 18 decimals
#         uint256 divisor = 10 ** IERC20Metadata(token).decimals(); // 18 always

#         // We are using the balances directly here which can be manipulated but these values are
#         // only used in the strategy where we do additional checks to ensure the pool
#         // is a good state
#         // slither-disable-next-line reentrancy-benign,reentrancy-no-eth
#         return pricer.getPriceInEth(token) * balances[index] / divisor; // 18e18
#     }


def getDestinationInfo_call(name: str, autopool_vault_address: str, destination_vault: str) -> Call:

    def handle_DestinationInfo(success, DestinationInfo):
        if success:
            cachedDebtValue, cachedMinDebtValue, cachedMaxDebtValue, lastReport, ownedShares = DestinationInfo
            return {
                "cachedDebtValue": cachedDebtValue / 1e18,
                "ownedShares": ownedShares / 1e18,
            }  # can be approx, exact is not required.

    return Call(
        autopool_vault_address,
        ["getDestinationInfo(address)((uint256,uint256,uint256,uint256,uint256))", destination_vault],
        [(name, handle_DestinationInfo)],
    )


def build_get_pool_id_call(name: str, balancer_pool_address: str) -> Call:
    return Call(balancer_pool_address, ["getPoolId()(bytes32)"], [(name, identity_with_bool_success)])


def build_getPoolTokens_call(name: str, pool_id):
    return Call(
        BALANCER_VAULT_ADDRESS,
        ["getPoolTokens(bytes32)(address[],uint256[],uint256)", pool_id],
        [
            (f"{name}_tokens", identity_with_bool_success),
            (f"{name}_balances", identity_with_bool_success),
            (f"{name}_last_change_block", identity_with_bool_success),
        ],
    )


def getPriceInEth_call(name: str, token_address: str) -> Call:
    return Call(
        ROOT_PRICE_ORACLE,
        ["getPriceInEth(address)(uint256)", token_address],
        [(name, safe_normalize_with_bool_success)],
    )


@st.cache_data(ttl=12 * 3600)
def build_balancer_autopool_asset_combination_calls(blocks) -> pd.DataFrame:
    destination_df = pd.read_parquet(ROOT_DIR / "vaults.parquet")
    destinations = Call(
        balETH_AUTOPOOL_ETH_ADDRESS, ["getDestinations()(address[])"], [("destinations", identity_function)]
    ).__call__(_w3=eth_client)["destinations"]

    destination_df = destination_df[destination_df["vaultAddress"].str.lower().isin(destinations)].copy()
    token_name_to_address = dict()
    for token_names, token_address in zip(destination_df["token_names"], destination_df["tokens"]):
        for token, address in zip(token_names, token_address):

            token_name_to_address[token] = address

    token_address_to_name = dict()
    for k, v in token_name_to_address.items():
        token_address_to_name[v] = k

    price_calls = [
        getPriceInEth_call(token_name, token_address) for token_name, token_address in token_name_to_address.items()
    ]

    pool_id_calls = [
        build_get_pool_id_call(name, pool) for name, pool in zip(destination_df["name"], destination_df["pool"])
    ]
    pool_ids = sync_get_raw_state_by_block_one_block(pool_id_calls, 20591987)

    get_pool_tokens_calls = [build_getPoolTokens_call(name, pool_ids[name]) for name in destination_df["name"]]  # fails

    price_df = sync_safe_get_raw_state_by_block(price_calls, blocks)
    get_pool_tokens_df = sync_safe_get_raw_state_by_block(get_pool_tokens_calls, blocks)
    # pool_id = build_get_pool_id_call('pool_id', '0x58AAdFB1Afac0ad7fca1148f3cdE6aEDF5236B6D').__call__(_w3=eth_client)['pool_id']
    # {'tokens': ('0x58aadfb1afac0ad7fca1148f3cde6aedf5236b6d', '0xa1290d69c65a6fe4df752f95823fae25cb99e5a7', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'), 'balances': (2596148429267437984416884399779299, 4192537420423622999208, 2534823097520260013367)}

    calls = [
        getDestinationInfo_call(name, balETH_AUTOPOOL_ETH_ADDRESS, vault)
        for (name, vault) in zip(destination_df["name"], destination_df["vaultAddress"])
    ]
    raw_ownedShares_df = sync_safe_get_raw_state_by_block(calls, blocks)

    cachedDebtValue_df = raw_ownedShares_df.map(
        lambda cell: cell["cachedDebtValue"] if isinstance(cell, dict) and "cachedDebtValue" in cell else 0
    )

    return price_df, get_pool_tokens_df, cachedDebtValue_df, token_address_to_name


def fetch_asset_composition_over_time_to_plot():
    blocks = build_blocks_to_use()
    price_df, get_pool_tokens_df, cachedDebtValue_df, token_address_to_name = (
        build_balancer_autopool_asset_combination_calls(blocks)
    )

    cachedDebtValue_df = cachedDebtValue_df.loc[:, (cachedDebtValue_df != 0).any(axis=0)]

    full_df = pd.concat([price_df, get_pool_tokens_df, cachedDebtValue_df], axis=1)
    full_df["timestamp"] = full_df.index
    destination_names_touched = cachedDebtValue_df.columns
    our_names_set = set()

    def _compute_asset_value_held(row: dict):
        assets_by_destination = []
        for destination_name in destination_names_touched:
            assets_in_destination_value = {}
            assets_in_destination_value["name"] = destination_name
            assets_in_destination_value["timestamp"] = row["timestamp"]
            assets_in_destination_value[f"our_approx_eth_value_in_{destination_name}"] = row[destination_name]
            total_value_in_destination = 0
            token_names = []
            for token_address, token_balance in zip(
                row[f"{destination_name}_tokens"], row[f"{destination_name}_balances"]
            ):
                token_address = eth_client.toChecksumAddress(token_address)
                if token_address in token_address_to_name:
                    token_name = token_address_to_name[token_address]
                    token_names.append(token_name)

                    token_price_in_eth = row[token_name]
                    token_quantity_normalized = token_balance / 1e18

                    token_value_in_eth_in_destination = token_quantity_normalized * token_price_in_eth
                    assets_in_destination_value[f"{token_name}_value_in_{destination_name}"] = (
                        token_value_in_eth_in_destination
                    )

                    total_value_in_destination += token_value_in_eth_in_destination

            assets_in_destination_value[f"total_value_in_{destination_name}"] = total_value_in_destination
            assets_in_destination_value[f"our_approx_portion_of_{destination_name}"] = (
                assets_in_destination_value[f"our_approx_eth_value_in_{destination_name}"]
                / assets_in_destination_value[f"total_value_in_{destination_name}"]
            )
            for token_name in token_names:
                our_names_set.add(f"{token_name}")
                assets_in_destination_value[f"{token_name}"] = (
                    assets_in_destination_value[f"our_approx_portion_of_{destination_name}"]
                    * assets_in_destination_value[f"{token_name}_value_in_{destination_name}"]
                )

            assets_by_destination.append(assets_in_destination_value)

        return assets_by_destination

    assets_by_destination_records = full_df.apply(_compute_asset_value_held, axis=1)
    flattened_list = [item for sublist in assets_by_destination_records for item in sublist]

    df = pd.DataFrame.from_records(flattened_list)
    df.set_index("timestamp", inplace=True)

    asset_df = df[list(our_names_set)]

    pie_df = asset_df.copy()
    pie_df["date"] = asset_df.index
    pie_data = pie_df.groupby("date").max().tail(1).T.reset_index()
    pie_data.columns = ["Asset", "ETH Value"]
    pie_data = pie_data[pie_data["ETH Value"] > 0]

    # pie chart
    asset_allocation_pie_fig = px.pie(
        pie_data, names="Asset", values="ETH Value", title=" ", color_discrete_sequence=px.colors.qualitative.Pastel
    )
    asset_allocation_pie_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=400,
        width=800,
        font=dict(size=16),
        legend=dict(font=dict(size=18), orientation="h", x=0.5, xanchor="center"),
        legend_title_text="",
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    asset_allocation_pie_fig.update_traces(textinfo="percent+label", hoverinfo="label+value+percent")

    # Normalize data for area chart
    asset_df = asset_df.div(asset_df.sum(axis=1), axis=0).fillna(0)

    #  area chart for token exposure over time
    asset_allocation_area_fig = px.bar(
        asset_df,
        title="",
        labels={"timestamp": "", "value": "Exposure Proportion"},
        color_discrete_sequence=px.colors.qualitative.Set1,
    )

    asset_allocation_area_fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=40),
        height=400,
        width=800,
        font=dict(size=16),
        xaxis_title=" ",
        yaxis_title="Proportion of Total Exposure",
        yaxis=dict(showgrid=True, gridcolor="lightgray"),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )

    return asset_allocation_area_fig, asset_allocation_pie_fig
