# the dominating time cost is in the log decoding, not certain on the way to speed it up.


from concurrent.futures import ProcessPoolExecutor

import numpy as np
import pandas as pd
from web3 import Web3
from web3.contract import ContractEvent
from web3._utils.events import get_event_data


EXPECTED_EVENT_FIELD_NAMES = [
    "event",
    "block",
    "hash",
    "transaction_index",
    "log_index",
]


def _flatten_args(args: dict) -> dict:
    out = {}
    for k, v in args.items():
        if isinstance(v, list):
            for i, vi in enumerate(v):
                out[f"{k}_{i}"] = vi
        else:
            out[k] = v
    return out


def _worker_decode_chunk(args) -> list[dict]:
    """
    Runs in a separate process. Decodes a *chunk* of logs to amortize process/pickle overhead.
    """
    abi, logs_chunk = args
    codec = Web3().codec  # lightweight; avoids pickling the original codec # according to chat
    decoded = []

    for log in logs_chunk:
        # minimal = {"topics": tuple(bytes.fromhex(t[2:]) for t in log["topics"]), "data": log["data"]}
        log["topics"] = [bytes.fromhex(t[2:]) for t in log["topics"]]
        ev = get_event_data(codec, abi, log)

        res = {
            "event": str(ev["event"]),
            "blockNumber": int(ev["blockNumber"], 16),
            "transactionIndex": int(ev["transactionIndex"], 16),
            "logIndex": int(ev["logIndex"], 16),
            "transactionHash": str(ev["transactionHash"]),
        }
        res.update(_flatten_args(ev["args"]))
        decoded.append(res)

    return decoded


def decode_logs(event: ContractEvent, raw_logs: list[dict]) -> pd.DataFrame:
    """
    Parallel CPU-bound decode using ProcessPoolExecutor with *batched* tasks.
    Preserves input order within each chunk; we concatenate in chunk order.
    """
    if len(raw_logs) > 0:
        abi = event._get_event_abi()

        # For small inputs, avoid multiprocessing overhead altogether.
        if len(raw_logs) < 5000:
            results = _worker_decode_chunk((abi, raw_logs))
        else:
            max_workers = 4
            chunks = np.array_split(np.array(raw_logs), max_workers)
            chunks = [(abi, list(chunk)) for chunk in chunks]
            results: list[dict] = []

            with ProcessPoolExecutor(max_workers=max_workers) as ex:
                for decoded_chunk in ex.map(_worker_decode_chunk, chunks):
                    results.extend(decoded_chunk)
    else:
        results = []

    df = pd.DataFrame(results)

    if df.empty:
        event_field_names = [i["name"] for i in event._get_event_abi()["inputs"]]
        return pd.DataFrame(columns=[*event_field_names, *EXPECTED_EVENT_FIELD_NAMES])

    df.rename(columns={"logIndex": "log_index", "transactionHash": "hash", "blockNumber": "block"}, inplace=True)
    df["hash"] = df["hash"].apply(lambda x: str.lower(x))
    df.sort_values("block", inplace=True)
    return df
