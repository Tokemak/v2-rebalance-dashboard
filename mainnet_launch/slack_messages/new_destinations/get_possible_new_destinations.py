import requests
import pandas as pd

from web3 import Web3
from multicall import Call

from mainnet_launch.constants import (
    ALL_AUTOPOOLS,
    AutopoolConstants,
    ETH_CHAIN,
    BASE_CHAIN,
    SONIC_CHAIN,
    ARBITRUM_CHAIN,
    PLASMA_CHAIN,
    LINEA_CHAIN,
    ALL_CHAINS,
)
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache
from mainnet_launch.data_fetching.get_state_by_block import get_state_by_one_block, identity_with_bool_success
from mainnet_launch.slack_messages.post_message import SlackChannel, post_message_with_table


DEFILLAMA_YIELDS_URL = "https://yields.llama.fi/pools"


class SlackMessagesNewDestinationsError(Exception):
    pass


def fetch_all_defillama_yields() -> pd.DataFrame:
    resp = requests.get(DEFILLAMA_YIELDS_URL, timeout=40, headers={"User-Agent": "defillama-yields-script"})
    resp.raise_for_status()
    payload = resp.json()
    df = pd.DataFrame.from_records(payload["data"])
    df["millions_usd"] = df["tvlUsd"] / 1_000_000
    return df


def fetch_an_autopools_valid_tokens(autopool: AutopoolConstants) -> list[str]:
    call = Call(
        autopool.autopool_eth_addr,
        ["getDestinations()(address[])"],
        [("destinations", identity_with_bool_success)],
    )

    destinations = list(
        get_state_by_one_block([call], autopool.chain.get_block_near_top(), autopool.chain)["destinations"]
    )
    destinations = [Web3.toChecksumAddress(addr) for addr in destinations]

    get_destination_tokens_query = f"""
        SELECT DISTINCT
            dt.token_address,
            t.symbol,
            t.name,
            t.decimals
        FROM destination_tokens AS dt
        JOIN tokens AS t
        ON t.token_address = dt.token_address
        AND t.chain_id     = dt.chain_id
        WHERE dt.destination_vault_address = ANY(ARRAY{destinations}::text[])
    """
    destination_tokens = _exec_sql_and_cache(get_destination_tokens_query)
    return destination_tokens["token_address"].tolist()


def get_valid_rows_for_autopool(
    autopool: AutopoolConstants,
    df: pd.DataFrame,
    tvl_threshold: float = 1_000_000,
) -> pd.DataFrame:
    autopool_tokens_lower = {str(t).lower() for t in fetch_an_autopools_valid_tokens(autopool)}

    def check_all_tokens_in_underlying(tokens, autopool_tokens_lower, project: str | None = None) -> bool:
        if not isinstance(tokens, list) or len(tokens) == 0:
            return False

        in_set_count = sum(1 for token in tokens if str(token).lower() in autopool_tokens_lower)
        n = len(tokens)

        project_str = (project or "").lower()
        if "aura" in project_str or "balancer" in project_str:
            # accounts for composable stable pool tokens (lp token is in the underlying)
            # note noisy, but ok for now
            return in_set_count >= n - 1

        return in_set_count == n

    underlying_tokens_in_autopool = df.apply(
        lambda row: check_all_tokens_in_underlying(row["underlyingTokens"], autopool_tokens_lower, row["project"]),
        axis=1,
    )

    chain_to_defi_llama_yield_chain_name = {
        ETH_CHAIN: "Ethereum",
        BASE_CHAIN: "Base",
        SONIC_CHAIN: "Sonic",
        ARBITRUM_CHAIN: "Arbitrum",
        PLASMA_CHAIN: "Plasma",
        LINEA_CHAIN: "Linea",
    }
    if len(ALL_CHAINS) != len(chain_to_defi_llama_yield_chain_name):
        raise SlackMessagesNewDestinationsError("chain_to_defi_llama_yield_chain_name mapping is incomplete")

    right_chain = df["chain"] == chain_to_defi_llama_yield_chain_name[autopool.chain]

    valid_protocols = [
        "morpho-v1",
        "morpho-v2",
        "morpho-v3",
        "convex-finance",
        "curve-dex",
        "aave-v3",
        "aave-v2",
        "fluid-lending",
        "silo-v2",
        "balancer-v3",
        "balancer-v2",
        "aura",
        "fluid-dex",
    ]

    valid_protocols_mask = df["project"].str.lower().isin(valid_protocols)

    valid_rows = df[
        underlying_tokens_in_autopool & right_chain & valid_protocols_mask & (df["tvlUsd"] >= tvl_threshold)
    ].copy()

    valid_rows["autopool_name"] = autopool.name
    return valid_rows


def fetch_possible_new_autopool_destinations(
    tvl_threshold: float = 1_000_000,
) -> pd.DataFrame:
    df = fetch_all_defillama_yields()

    valid_rows_list = []
    for autopool in ALL_AUTOPOOLS:
        valid_rows = get_valid_rows_for_autopool(autopool, df, tvl_threshold)
        valid_rows_list.append(valid_rows)

    all_valid_rows_df = pd.concat(valid_rows_list, axis=0).reset_index(drop=True)

    return all_valid_rows_df


def extract_possible_interesting_destinations(df: pd.DataFrame) -> pd.DataFrame:
    indexes = set()

    for autopool in ALL_AUTOPOOLS:
        sub_df = df[df["autopool_name"] == autopool.name]
        if not sub_df.empty:
            indexes.update([*sub_df["apyMean30d"].nlargest(2).index, *sub_df["apy"].nlargest(2).index])

    interesting_df = df.loc[list(indexes)].reset_index(drop=True)
    interesting_df = (
        interesting_df.drop_duplicates(subset=["project", "symbol"])
        .sort_values(by=["apy"], ascending=False)
        .reset_index(drop=True)
    )
    interesting_df["copy_to_search"] = (
        interesting_df["project"] + " " + interesting_df["symbol"] + " " + interesting_df["chain"]
    )
    interesting_columns = [
        "copy_to_search",
        "project",
        "symbol",
        "apyMean30d",
        "apy",
        "millions_usd",
        "autopool_name",
    ]

    interesting_df["apyMean30d"] = interesting_df["apyMean30d"].map(lambda x: f"{x:.2f}%")
    interesting_df["apy"] = interesting_df["apy"].map(lambda x: f"{x:.2f}%")
    interesting_df["millions_usd"] = interesting_df["millions_usd"].map(lambda x: f"${x:.2f}M")

    return interesting_df[interesting_columns]


def post_possible_new_destinations(slack_channel: SlackChannel):
    df = fetch_possible_new_autopool_destinations(tvl_threshold=2_000_000)
    interesting_df = extract_possible_interesting_destinations(df)

    if not interesting_df.empty:
        post_message_with_table(
            channel=slack_channel,
            initial_comment=f"{len(interesting_df)} Possible New Autopool Destinations (TVL >= $2M):",
            df=interesting_df,
            file_save_name="Possible Autopool Destinations.csv",
        )


if __name__ == "__main__":
    post_possible_new_destinations()
