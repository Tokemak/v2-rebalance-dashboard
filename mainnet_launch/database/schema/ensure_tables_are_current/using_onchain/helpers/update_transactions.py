"""Helper to idempotently ensure that all transactions are saved in the Transactions table"""

import numpy as np
import requests
import json


from mainnet_launch.database.schema.full import Transactions
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    ensure_all_blocks_are_in_table,
)
from mainnet_launch.database.postgres_operations import insert_avoid_conflicts, get_subset_not_already_in_column
from mainnet_launch.constants import ChainData, DEAD_ADDRESS, time_decorator
from tqdm import tqdm
from mainnet_launch.database.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)


def fetch_transaction_rows_bulk_from_alchemy(tx_hashes: list[str], chain: ChainData) -> list[Transactions]:
    def hex_to_int(hexstr: str) -> int:
        return int(hexstr, 16)

    batch_size = 50
    num_batches = (len(tx_hashes) + batch_size - 1) // batch_size

    tx_hash_groups = np.array_split(tx_hashes, num_batches)
    all_found_transactions = []
    for tx_group in tqdm(tx_hash_groups, desc=f"Fetching transactions for {chain.name} in bulk from alchemy"):
        batch_payload = [
            {"jsonrpc": "2.0", "id": str(tx_hash), "method": "eth_getTransactionReceipt", "params": [str(tx_hash)]}
            for tx_hash in tx_group
        ]

        headers = {"Content-Type": "application/json"}
        response = requests.post(chain.client.provider.endpoint_uri, data=json.dumps(batch_payload), headers=headers)
        response.raise_for_status()
        responses = response.json()

        def _record_to_transaction(tx_receipt: dict) -> Transactions:
            tx = tx_receipt["result"]
            gas_used = hex_to_int(tx["gasUsed"])
            effective_gas_price = hex_to_int(tx["effectiveGasPrice"])

            to_address = tx["to"]
            if to_address is None:
                # here a dead address means a contract creation transaction
                # alchemy returns None for contract creation transactions
                # for the `to` field
                to_address = DEAD_ADDRESS
            else:
                to_address = chain.client.toChecksumAddress(to_address)

            from_address = chain.client.toChecksumAddress(tx["from"])
            return Transactions(
                tx_hash=tx["transactionHash"],
                block=hex_to_int(tx["blockNumber"]),
                chain_id=chain.chain_id,
                from_address=from_address,
                to_address=to_address,
                effective_gas_price=effective_gas_price,
                gas_used=gas_used,
                gas_cost_in_eth=(gas_used * effective_gas_price) / 10**18,
            )

        found_transactions = [_record_to_transaction(tx_receipt) for tx_receipt in responses]
        all_found_transactions.extend(found_transactions)

    return all_found_transactions


def ensure_all_transactions_are_saved_in_db(tx_hashes: list[str], chain: ChainData) -> None:
    """
    Idempotently ensure that all tx_hashes are saved in the Transactions table
    Also ensures that all blocks for those transactions are saved in the Blocks table
    """
    if not isinstance(tx_hashes, list):
        raise TypeError("tx_hashes must be a list")

    local_tx_hashes = [h.lower() for h in tx_hashes]

    # this is slow with 5k hashes,
    hashes_to_fetch = get_subset_not_already_in_column(
        Transactions,
        Transactions.tx_hash,
        values=local_tx_hashes,
        where_clause=Transactions.chain_id == chain.chain_id,  # where not needed but (might) speed up query
    )

    if not hashes_to_fetch:
        return
    print(f"Fetching {len(hashes_to_fetch)} new transactions for {chain.name}")

    chunk_size = 5_000
    for i in range(0, len(hashes_to_fetch), chunk_size):
        chunk = hashes_to_fetch[i : i + chunk_size]
        print(f"Processing chunk {i//chunk_size + 1}/{(len(hashes_to_fetch) + chunk_size - 1)//chunk_size}")

        new_transactions: list[Transactions] = fetch_transaction_rows_bulk_from_alchemy(chunk, chain)

        ensure_all_blocks_are_in_table([t.block for t in new_transactions], chain)
        insert_avoid_conflicts(new_transactions, Transactions) # I think this writing is the slow part
        print(f"Inserted {len(new_transactions)} transactions for {chain.name}")

    print(f"Completed inserting all {len(hashes_to_fetch)} new transactions for {chain.name}")
