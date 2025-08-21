import requests
import json
import numpy as np
from web3 import Web3
from mainnet_launch.constants.constants import ChainData


# not used
def batch_get_to_address_by_transaction_hash(tx_hashes: list[str], chain: ChainData) -> dict[str, str]:
    # returns a diction of txHash:toAddress
    # there can be no more than 1000 batch request per call
    num_batches = 1 + (len(tx_hashes) // 1000)

    tx_hash_groups = np.array_split(tx_hashes, num_batches)
    # int(x, 16) I think does it
    tx_hash_mapping_to_address = {}
    for tx_group in tx_hash_groups:
        batch_payload = [
            {"jsonrpc": "2.0", "id": tx_hash, "method": "eth_getTransactionByHash", "params": [tx_hash]}
            for tx_hash in tx_group
        ]

        headers = {"Content-Type": "application/json"}
        response = requests.post(chain.client.provider.endpoint_uri, data=json.dumps(batch_payload), headers=headers)
        response.raise_for_status()
        responses = response.json()
        # gasPrice and gasUsed
        print(responses[0].keys())

        results_by_id = {res["id"]: Web3.toChecksumAddress(res["result"]["to"]) for res in responses}
        tx_hash_mapping_to_address.update(results_by_id)

    return tx_hash_mapping_to_address
