"""

Each Multicall is an:

- ordered list of calls
- on a chain
- at a block


Assuming the call is performed on after the chain reaches finality:

- The same Multicall on the same chain, on the same block will always give the same response'

So we can sha256(Multicall) -> response, and save that locally 

# then check if we have it, if so read locally, else fetch, then save and return 

Calls are defined as:

- target_address
- function
- function args
- handling function (ie safe_normalize)

"""

import hashlib
import inspect
import time
from functools import lru_cache
from mainnet_launch.constants import ChainData
from multicall import Multicall, Call


@lru_cache(maxsize=None)
def _get_function_source(fn):
    """get the plain ttext of a handling operation such as safe normalize"""
    return inspect.getsource(fn)


def _serialize_call(call: Call) -> bytes:

    # Serialize target address: 20 bytes (assuming '0x' prefix)
    target_bytes = bytes.fromhex(call.target.lower()[2:])

    # Serialize data: variable length
    if isinstance(call.data, bytes):
        data_bytes = call.data
    else:
        raise TypeError(f"call.data must be bytes, got {type(call.data)}")

    # Serialize function name: UTF-8 encoded
    function_bytes = call.function.encode("utf-8")

    # Serialize returns_functions: list of (name, source) tuples
    returns_functions_bytes = b"".join(
        [
            name.encode("utf-8") + b"\x00" + _get_function_source(fn).encode("utf-8") + b"\x00"
            for name, fn in call.returns
        ]
    )

    # Combine all parts with clear separators or fixed lengths
    serialized = (
        len(target_bytes).to_bytes(4, "big")
        + target_bytes
        + len(data_bytes).to_bytes(4, "big")
        + data_bytes
        + len(function_bytes).to_bytes(4, "big")
        + function_bytes
        + len(returns_functions_bytes).to_bytes(4, "big")
        + returns_functions_bytes
    )

    return serialized


def _serialize_multicall(multicall: Multicall) -> bytes:
    """
    Serialize a Multicall object into bytes.
    don't include the block id

    Parameters:
        multicall (Multicall): The Multicall object to serialize.

    Returns:
        bytes: The serialized byte representation of the Multicall object.
    """
    parts = []

    # Serialize chainid: 8 bytes (big endian)
    parts.append(multicall.chainid.to_bytes(8, "big"))

    # Serialize require_success: 1 byte (0x01 for True, 0x00 for False)
    parts.append(b"\x01" if multicall.require_success else b"\x00")

    # Serialize multicall_address: 20 bytes (assuming '0x' prefix)
    multicall_address_bytes = bytes.fromhex(multicall.multicall_address.lower()[2:])
    parts.append(multicall_address_bytes)

    # Serialize each Call object
    for call in multicall.calls:
        call_bytes = _serialize_call(call)
        parts.append(call_bytes)

    # Combine all parts into a single byte sequence
    serialized_multicall = b"".join(parts)
    return serialized_multicall


def _multicall_to_db_hash_optimized(static_hash: bytes, block_id: int) -> str:
    """
    Generate a SHA-256 hash for a given block_id using the precomputed static hash.

    Parameters:
        static_hash (bytes): The precomputed SHA-256 hash of the multicall (excluding block_id).
        block_id (int): The block ID to include in the final hash.

    Returns:
        str: The final SHA-256 hash as a hexadecimal string.
    """
    # Serialize the block_id
    block_id_bytes = block_id.to_bytes(8, "big")  # 8 bytes for consistency

    # Combine static hash with block_id
    combined = static_hash + block_id_bytes

    # Compute the final SHA-256 hash
    final_hash = hashlib.sha256(combined).hexdigest()

    return final_hash


def calls_and_blocks_to_db_hashes(calls: list[Call], blocks: list[int], chain: ChainData) -> list[str]:
    """
    Efficiently compute the db_hashes for a list of multicalls

    Note: requires (but does not check) that its the same set of calls on teh same chain at differn blocks

    """

    multicalls = [Multicall(calls=calls, block_id=b, _w3=chain.client, require_success=False) for b in blocks]
    # one hash for all the info except the block info
    serialized_static_multicall = _serialize_multicall(multicalls[0])
    static_hash = hashlib.sha256(serialized_static_multicall).digest()
    db_hashes = [_multicall_to_db_hash_optimized(static_hash, mc.block_id) for mc in multicalls]
    return db_hashes
