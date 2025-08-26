from mainnet_launch.constants import ALL_AUTOPOOLS, AutopoolConstants, BAL_ETH
from mainnet_launch.abis import AUTOPOOL_VAULT_WITH_FEE_COLLECTED_EVENT_ABI
from mainnet_launch.database.schema.full import AutopoolFees
from mainnet_launch.database.schema.postgres_operations import simple_agg_by_one_table

from mainnet_launch.data_fetching.get_events import fetch_events


def get_highest_already_fetched_autopool_fees_block() -> int:
    most_recent_block_with_fee = simple_agg_by_one_table(
        table=AutopoolFees,
        agg_column=AutopoolFees.block,
        agg_function="max",
        group_by_column=AutopoolFees.autopool_vault_address,
    )

    return most_recent_block_with_fee


def _fetch_fee_events_for_autopool(
    autopool: AutopoolConstants,
    start_block: int,
    end_block: int,
) -> list[dict]:
    contract = autopool.chain.client.eth.contract(
        address=autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_WITH_FEE_COLLECTED_EVENT_ABI
    )
    events = fetch_events(
        contract.events.FeeCollected,
        chain=autopool.chain,
        start_block=start_block,
        end_block=end_block,
    )
    return events


if __name__ == "__main__":
    df = _fetch_fee_events_for_autopool(BAL_ETH, BAL_ETH.block_deployed, 23227664 - 100)
    pass
