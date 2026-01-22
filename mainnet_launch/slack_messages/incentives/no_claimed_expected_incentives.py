import pandas as pd
from pprint import pprint

from mainnet_launch.constants import ALL_AUTOPOOLS
from mainnet_launch.database.postgres_operations import (
    get_full_table_as_df_with_tx_hash,
    get_full_table_as_df_with_block,
    get_full_table_as_df,
)
from mainnet_launch.database.schema.full import (
    Blocks,
    IncentiveTokenBalanceUpdated,
    Tokens,
    DestinationStates,
    AutopoolDestinationStates,
    AutopoolDestinations,
    Destinations,
)
from mainnet_launch.slack_messages.post_message import (
    SlackChannel,
    PostSlackMessageError,
    post_message_with_table,
    post_slack_message,
)

# TODO not accurate, false positives and negatives


def augment_claimed_events(claimed: pd.DataFrame) -> pd.DataFrame:
    claimed = claimed.copy()
    claimed["symbol"] = claimed["destination_symbol"] + " - " + claimed["token_symbol"]
    claimed["block_timestamp"] = pd.to_datetime(claimed.index, utc=True)
    claimed = claimed.sort_values(["symbol", "block_timestamp"])
    claimed["hours_between"] = claimed.groupby("symbol")["block_timestamp"].diff().dt.total_seconds() / 3600.0
    return claimed


def fetch_incentive_token_claimed_data():
    destinations = get_full_table_as_df(Destinations)
    tokens = get_full_table_as_df(Tokens)
    autopool_destinations = get_full_table_as_df(AutopoolDestinations)

    # TODO maybe don't fetch all the data, only 2 months? only 1 month?
    claimed = get_full_table_as_df_with_tx_hash(IncentiveTokenBalanceUpdated)
    a_week_ago = pd.Timestamp.now(tz="utc") - pd.Timedelta(days=7)

    destination_states = get_full_table_as_df_with_block(DestinationStates, where_clause=Blocks.datetime > a_week_ago)
    autopool_states = get_full_table_as_df_with_block(
        AutopoolDestinationStates, where_clause=Blocks.datetime > a_week_ago
    )

    destination_vault_address_to_symbol = dict(
        zip(destinations["destination_vault_address"], destinations["underlying_name"])
    )
    token_address_to_symbol = dict(zip(tokens["token_address"], tokens["symbol"]))
    autopool_vault_address_to_symbol = {a.autopool_eth_addr: a.name for a in ALL_AUTOPOOLS}

    claimed["destination_symbol"] = claimed["destination_vault_address"].map(destination_vault_address_to_symbol)
    claimed["token_symbol"] = claimed["token_address"].map(token_address_to_symbol)

    destination_states["destination_symbol"] = destination_states["destination_vault_address"].map(
        destination_vault_address_to_symbol
    )

    autopool_states["destination_symbol"] = autopool_states["destination_vault_address"].map(
        destination_vault_address_to_symbol
    )
    autopool_states["autopool_name"] = autopool_states["autopool_vault_address"].map(autopool_vault_address_to_symbol)

    autopool_destinations["destination_symbol"] = autopool_destinations["destination_vault_address"].map(
        destination_vault_address_to_symbol
    )
    autopool_destinations["autopool_name"] = autopool_destinations["autopool_vault_address"].map(
        autopool_vault_address_to_symbol
    )

    claimed = augment_claimed_events(claimed)
    return claimed, destination_states, autopool_states, autopool_destinations


def identify_suspect_destinations(
    claimed: pd.DataFrame,
    destination_states: pd.DataFrame,
    autopool_states: pd.DataFrame,
    autopool_destinations: pd.DataFrame,
) -> pd.DataFrame:
    """
    A destination is suspect if:
        - We owned >0 shares at any point in the last week
        - We showed a >0 incentive APR at any point in the last week
        - The liquidation row has not emitted a balanceUpdated event in the last 3 days for that destination
    """
    three_days_ago = pd.Timestamp.now(tz="utc") - pd.Timedelta(days=3)
    recently_claimed_destinations = claimed[claimed.index > three_days_ago]["destination_vault_address"].unique()
    destinations_with_some_shares = (
        autopool_states.groupby("destination_vault_address")["owned_shares"].max().loc[lambda x: x > 0].index
    )
    destinations_with_some_incentive_apr = (
        destination_states.groupby("destination_vault_address")["incentive_apr"].max().loc[lambda x: x > 0].index
    )

    destinations_with_some_incentive_apr_and_owned_shares = [
        s for s in destinations_with_some_incentive_apr if s in destinations_with_some_shares
    ]
    expected_but_not_claimed = [
        s for s in destinations_with_some_incentive_apr_and_owned_shares if s not in recently_claimed_destinations
    ]
    suspect_destinations = autopool_destinations[
        autopool_destinations["destination_vault_address"].isin(expected_but_not_claimed)
    ]

    suspect_destinations = suspect_destinations.merge(
        destination_states.groupby("destination_vault_address")["incentive_apr"].max().rename("recent_incentive_apr")
        * 100,
        on="destination_vault_address",
        how="left",
    )

    most_owned_shares = (
        autopool_states.groupby("destination_vault_address")["owned_shares"].max().rename("most_owned_shares")
    )
    suspect_destinations = suspect_destinations.merge(most_owned_shares, on="destination_vault_address", how="left")

    most_recent_claimed = (
        claimed.groupby("destination_vault_address")["block_timestamp"].max().rename("most_recent_claimed")
    )
    suspect_destinations = suspect_destinations.merge(most_recent_claimed, on="destination_vault_address", how="left")

    suspect_destinations["days_since_claim"] = (
        pd.Timestamp.now(tz="utc") - suspect_destinations["most_recent_claimed"]
    ).dt.total_seconds() / (3600.0 * 24.0)

    cols = [
        "destination_symbol",
        "days_since_claim",
        "recent_incentive_apr",
        "most_owned_shares",
        "autopool_name",
        "destination_vault_address",
    ]

    if len(suspect_destinations["destination_vault_address"].drop_duplicates()) != len(expected_but_not_claimed):
        pprint(suspect_destinations)
        # note: might break on destinations that we should have claimed, but never had claimed
        raise PostSlackMessageError(
            f"Expected {len(expected_but_not_claimed)} suspect destinations, but only found {len(suspect_destinations)}"
        )

    return suspect_destinations[cols].round(4).sort_values("days_since_claim", ascending=False)


def post_missing_balance_updated_events(slack_channel: SlackChannel):
    claimed, destination_states, autopool_states, autopool_destinations = fetch_incentive_token_claimed_data()
    suspect_destinations = identify_suspect_destinations(
        claimed, destination_states, autopool_states, autopool_destinations
    )
    if suspect_destinations.empty:
        post_slack_message(
            slack_channel,
            "All destinations with incentive APR and owned shares have had claimed incentive token balance updates in the last 3 days",
        )
        return

    # we should get a weeks worth of warning before we start getting false positives
    initial_comment = """
        The following destinations have had incentive APR and owned shares in the last week, but have not had any claimed incentive token balance updates in the last 3 days
        Can have false positives
    """

    post_message_with_table(
        slack_channel,
        initial_comment=initial_comment,
        df=suspect_destinations,
        file_save_name="Suspect Destinations Missing Claimed Incentive Token Balance Updates.csv",
    )


if __name__ == "__main__":
    post_missing_balance_updated_events(SlackChannel.TESTING)
