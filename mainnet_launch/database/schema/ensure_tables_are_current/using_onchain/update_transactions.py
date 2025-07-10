from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import requests
import json

from mainnet_launch.database.schema.full import Transactions
from mainnet_launch.data_fetching.block_timestamp import ensure_all_blocks_are_in_table
from mainnet_launch.database.schema.postgres_operations import insert_avoid_conflicts, get_subset_not_already_in_column
from mainnet_launch.constants import ChainData
from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)


def _fetch_new_transaction_row(tx_hash: str, chain: ChainData) -> Transactions:
    tx_hash = tx_hash.lower()
    tx_receipt = chain.client.eth.get_transaction_receipt(tx_hash)
    # effectiveGasPrice replaces gasPrice after 2021 because of the London Fork
    # not certain where it is in alchemy
    return Transactions(
        tx_hash=tx_hash,
        block=tx_receipt["blockNumber"],
        chain_id=chain.chain_id,
        from_address=tx_receipt["from"],
        to_address=tx_receipt["to"],
        effective_gas_price=tx_receipt["effectiveGasPrice"],
        gas_used=tx_receipt["gasUsed"],
        gas_cost_in_eth=(tx_receipt["gasUsed"] * tx_receipt["effectiveGasPrice"]) / 1e18,
    )


# way to slow
def _fetch_all_new_transaction_records(tx_hashes: list[str], chain: ChainData) -> list[Transactions]:
    new_transactions_records: list[Transactions] = []
    hashes_to_fetch = get_subset_not_already_in_column(
        Transactions, Transactions.tx_hash, values=tx_hashes, where_clause=Transactions.chain_id == chain.chain_id
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = {executor.submit(_fetch_new_transaction_row, h, chain): h for h in hashes_to_fetch}
        for future in as_completed(futures):
            new_transactions_records.append(future.result())

    return new_transactions_records


# def batch_get_transaction receipts(tx_hashes: list[str], chain: ChainData) -> list[Transactions]
#     # returns a diction of txHash:toAddress
#     # there can be no more than 1000 batch request per call
#     num_batches = 1 + (len(tx_hashes) // 1000)

#     tx_hash_groups = np.array_split(tx_hashes, num_batches)
#     # int(x, 16) I think does it
#     tx_hash_mapping_to_address = {}
#     for tx_group in tx_hash_groups:
#         batch_payload = [
#             {"jsonrpc": "2.0", "id": tx_hash, "method": "eth_getTransactionReceipt", "params": [tx_hash]}
#             for tx_hash in tx_group
#         ]

#         headers = {"Content-Type": "application/json"}
#         response = requests.post(chain.client.provider.endpoint_uri, data=json.dumps(batch_payload), headers=headers)
#         response.raise_for_status()
#         responses = response.json()
#         # gasPrice and gasUsed
#         print(responses[0].keys())

#     return tx_hash_mapping_to_address


def ensure_all_transactions_are_saved_in_db(tx_hashes: list[str], chain: ChainData) -> None:
    if not isinstance(tx_hashes, list):
        raise TypeError("tx_hashes must be a list")

    new_transactions: list[Transactions] = _fetch_all_new_transaction_records(tx_hashes, chain)

    if not new_transactions:
        return

    ensure_all_blocks_are_in_table([t.block for t in new_transactions], chain)
    insert_avoid_conflicts(new_transactions, Transactions, index_elements=[Transactions.tx_hash])
