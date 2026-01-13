"""

Note you can't use alchemy here because it doesn't get all the trasactions only the asset transfers


For Etherscan you need to use the v2 endpoint use
ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"

not
ETHERSCAN_API_URL = "https://api.etherscan.io/api"
the old API silently misbehaves

"""

import pandas as pd
import time

from mainnet_launch.constants import ChainData, ETHERSCAN_API_KEY, ETHERSCAN_API_URL
from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import make_single_request_to_3rd_party


class EtherscanAPIError(Exception):
    pass


def _fetch_pages(
    chain: ChainData,
    address: str,
    start: int,
    end: int,
    offset: int = 1000,
) -> tuple[list[dict], bool]:
    """
    Fetch pages 1-10 for [start…end]. Returns (tx_list, hit_limit).
    hit_limit==True if page 10 returned a full batch (i.e. you may have more).
    """
    txs = []
    for page in range(1, 11):
        params = {
            "module": "account",
            "action": "txlist",
            "chainid": chain.chain_id,
            "address": address,
            "startblock": start,
            "endblock": end,
            "page": page,
            "offset": offset,
            "sort": "asc",
            "apikey": ETHERSCAN_API_KEY,
        }

        def custom_failure_function(response_data: dict) -> bool:
            # Etherscan returns 200 OK even on errors, so we need to check the "status" field
            if int(response_data.get("status")) == 0:
                return True
            else:
                return False

        resp = make_single_request_to_3rd_party(
            {
                "method": "GET",
                "url": ETHERSCAN_API_URL,
                "params": params,
            },
            custom_failure_function=custom_failure_function,
        )

        batch = resp.get("result", [])
        if not batch:
            return txs, False
        txs.extend(batch)
        if len(batch) < offset:
            return txs, False
    # if we made it through 10 full pages, we hit the 10,000 record cap
    return txs, True


def _get_normal_transactions_from_etherscan_recursive(
    chain: ChainData, address: str, start: int, end: int
) -> list[dict]:
    """
    Recursively page through [start…end]. If you hit the record cap,
    advance start to the highest block seen +1 and recurse.
    """
    all_txs, hit_limit = _fetch_pages(chain, address, start, end)
    if not hit_limit:
        return all_txs

    # We fetched 10 full pages => there are more transactions in [start…end]
    max_block = max(int(tx["blockNumber"]) for tx in all_txs)
    # Recurse from just past the highest block

    time.sleep(0.2)  # be kind to Etherscan
    return all_txs + _get_normal_transactions_from_etherscan_recursive(chain, address, max_block + 1, end)


def get_all_transactions_sent_by_eoa_address(
    chain: ChainData,
    EOA_address: str,
    from_block: int,
    to_block: int,
) -> pd.DataFrame:
    """Use pagination to get *all* internal txns sent by `EOA_address` from Etherscan,
    note, no concurrency, or rate limiting. Make sure to add it later"""

    all_txs = _get_normal_transactions_from_etherscan_recursive(chain, EOA_address, from_block, to_block)
    df = pd.DataFrame.from_records(all_txs)
    if df.empty:
        return df
    # we only care about transactions sent by the EOA address
    # the etherscan endpoint returns all normal transactions where the EOA is in the `to` or `from` field
    if "from" not in df.columns:
        print(df.columns)
        print(df.head())
        print(df.shape)
        print(df.tail)
        raise EtherscanAPIError(
            f"Etherscan response missing 'from' field for: \n {EOA_address=} {chain.name=} {from_block=} {to_block=}"
        )

    df["from"] = df["from"].apply(lambda x: chain.client.toChecksumAddress(x))
    df = df[df["from"] == chain.client.toChecksumAddress(EOA_address)].copy()
    return df


if __name__ == "__main__":
    breaking_address = "0x241b8f1fA50F1Ce8fb78e7824757280feEb2aea3"

    from mainnet_launch.constants import ETH_CHAIN

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=10) as exe:

        addresses = [
            "0x5020c6EB0fE5321071942847B56349a68C7342dD",
            "0x14d97603B995f1f433341441cAA83ce5239aD2d3",
            "0x67beb3Dd509b88b706dC5A9f03f50006410b088B",
            "0x5416808256eA66367d7Ec1Ae2C37BB64EC2425d4",
            "0x6E21DBf061FDCdc8D3695150edb384ce2E590d48",
            "0x9a7cc0bd4BFce8A031ce02D56acf5E0a8c2e3F61",
            "0xB8B1be69A221Ce7b747ce71f262C0B18Bc60df19",
            "0x925dB2228A00f4bC0Fb627618e71542ECdd24B17",
            "0xa9FFE7DBE8cb20F493Dbf875fF0FdB10FeDbcc24",
            "0x30f29Ca88311F4cc1A1314bc1c45752982D7FD67",
        ] * 10

        futures = [
            exe.submit(
                get_all_transactions_sent_by_eoa_address,
                ETH_CHAIN,
                address,
                ETH_CHAIN.block_autopool_first_deployed,
                ETH_CHAIN.get_block_near_top(),
            )
            for address in addresses
        ]

        for future in futures:
            tx_df = future.result()
            print(tx_df.shape)
