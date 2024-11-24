import sqlite3
import hashlib
import inspect
from functools import lru_cache
import concurrent.futures
import time
from typing import List

from multicall import Multicall, Call
import pandas as pd

from mainnet_launch.constants import (
    CACHE_TIME,
    ChainData,
    TokemakAddress,
    time_decorator,
    ETH_CHAIN,
    BASE_CHAIN
)

MULTICALL_V3 = TokemakAddress(
    eth="0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696",
    base="0xcA11bde05977b3631167028862bE2a173976CA11"
)

# Cache for handling function sources
@lru_cache(maxsize=None)
def _get_function_source(fn):
    return inspect.getsource(fn)


def _serialize_call(call: Call) -> bytes:
    """
    Serialize a Call object into bytes.
    """

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


def _serialize_multicall(multicall: Multicall, include_block_id: bool = True) -> bytes:
    """
    Serialize a Multicall object into bytes.

    Parameters:
        multicall (Multicall): The Multicall object to serialize.
        include_block_id (bool): Whether to include the block_id in serialization.

    Returns:
        bytes: The serialized byte representation of the Multicall object.
    """
    parts = []

    if include_block_id:
        # Serialize block_id: 8 bytes (big endian)
        parts.append(multicall.block_id.to_bytes(8, "big"))

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



def multicall_to_db_hash_optimized(static_hash: bytes, block_id: int) -> str:
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


def serialize_multicalls_with_static_hash(multicalls:list[Multicall]) -> list[str]:
    
    serialized_static_multicall = _serialize_multicall(multicalls[0], include_block_id=False)
    static_hash = hashlib.sha256(serialized_static_multicall).digest()  # Use digest for binary data
    db_hashes = [multicall_to_db_hash_optimized(static_hash, mc.block_id) for mc in multicalls]
    return db_hashes


def identity_with_bool_success(success, value):
    if success:
        return value
    return None


def make_a_bunch_of_small_multicalls(n_blocks: int, num_extra_calls: int = 10) -> List[Multicall]:
    """
    Generate multiple Multicall objects, each containing a set of Call objects with unique function identifiers.

    Each Multicall includes:
        - get_timestamp_call
        - get_block_call
        - num_extra_calls additional calls with unique 'function' identifiers ("1", "2", ..., "num_extra_calls")

    Parameters:
        n_blocks (int): Number of blocks for which to generate Multicall objects.
        num_extra_calls (int): Number of additional Call objects with unique function identifiers per Multicall.

    Returns:
        List[Multicall]: A list of generated Multicall objects.
    """
    base_multicall_address = MULTICALL_V3.eth  # Assuming 'eth' is the desired chain

    # Define base calls for timestamp and block number
    get_timestamp_call = Call(
        base_multicall_address,
        "getCurrentBlockTimestamp()(uint256)",
        [("timestamp", identity_with_bool_success)],
    )
    get_block_call = Call(
        base_multicall_address,
        "getBlockNumber()(uint256)",
        [("block", identity_with_bool_success)],
    )

    # Generate additional calls with unique 'function' identifiers ("1", "2", ..., "num_extra_calls")
    # Replace "getSomeData()(uint256)" with actual function signatures as needed
    additional_calls = [
        Call(
            base_multicall_address,
            "getSomeData()(uint256)",  # Example function signature; replace as necessary
            [(str(i), identity_with_bool_success)]
        )
        for i in range(1, num_extra_calls + 1)
    ]

    # Combine base calls with additional unique calls
    all_calls = [get_timestamp_call, get_block_call] + additional_calls

    # Define the range of blocks for which to create Multicall objects
    start_block = 10_000_000
    blocks = list(range(start_block, start_block + n_blocks))

    # Create Multicall objects for each block
    all_multicalls = [
        Multicall(
            calls=all_calls,
            block_id=block,
            _w3=BASE_CHAIN.client,
            require_success=False,
        )
        for block in blocks
    ]
    return all_multicalls


def benchmark_multicall_hashing(n_blocks: int, num_extra_calls: int):
    """
    Benchmark the time taken to generate and hash Multicall objects both serially and in parallel.

    Parameters:
        n_blocks (int): Number of blocks for which to generate Multicall objects.
        num_extra_calls (int): Number of additional Call objects with unique function identifiers per Multicall.
        max_workers (int): Number of parallel worker processes for hashing.

    Returns:
        None
    """
    print(f"Benchmarking with {n_blocks} blocks, {num_extra_calls} extra calls per multicall")

    # Measure Multicall Generation Time
    start_time = time.perf_counter()
    all_multicalls = make_a_bunch_of_small_multicalls(n_blocks=n_blocks, num_extra_calls=num_extra_calls)
    generation_time = time.perf_counter() - start_time
    print(f"Generated {n_blocks} multicalls in {generation_time:.4f} seconds.")

    # Measure Serial Hashing Time
    start_time = time.perf_counter()
    h1 = serialize_multicalls_with_static_hash(all_multicalls)
    serial_hashing_time = time.perf_counter() - start_time
    print(f" serialize_multicalls_with_static_hash {n_blocks} multicalls in {serial_hashing_time:.4f} seconds.")
    print(f"Average serial hashing time per multicall: {serial_hashing_time / n_blocks:.6f} seconds.")
    h2 = serialize_multicalls_with_static_hash(all_multicalls)
    assert h1 == h2


if __name__ == '__main__':
    # Define benchmark configurations
    benchmark_configs = [
        {"n_blocks": 1, "num_extra_calls": 1000,},
        {"n_blocks": 10, "num_extra_calls": 1000, },
        {"n_blocks": 1000 * 1000, "num_extra_calls": 1000, }
    ]

    print("=== Optimized Benchmarking ===")
    for config in benchmark_configs:
        benchmark_multicall_hashing(
            n_blocks=config["n_blocks"],
            num_extra_calls=config["num_extra_calls"],
        )

















# import sqlite3
# import hashlib
# import inspect
# from functools import lru_cache
# import concurrent.futures
# import time

# from multicall import Multicall, Call
# import pandas as pd


# from mainnet_launch.constants import CACHE_TIME, ChainData, TokemakAddress, time_decorator, ETH_CHAIN, BASE_CHAIN

# MULTICALL_V3 = TokemakAddress(
#     eth="0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696", base="0xcA11bde05977b3631167028862bE2a173976CA11"
# )


# # Cache for handling function sources
# @lru_cache(maxsize=None)
# def _get_function_source(fn):
#     return inspect.getsource(fn)


# def _serialize_call(call: Call) -> bytes:
#     """
#     Serialize a Call object into bytes.
#     """

#     # Serialize target address: 20 bytes (assuming '0x' prefix)
#     target_bytes = bytes.fromhex(call.target.lower()[2:])

#     # Serialize data: variable length
#     # print(call.data)
#     # data_bytes = bytes.fromhex(call.data) if call.data.startswith("0x") else call.data.encode("utf-8")
#     if isinstance(call.data, bytes):
#         # I think this is right but am not 100% on it, need to test with different arguments
#         data_bytes = call.data
#     else:
#         raise TypeError(f"call.data must be bytes, got {type(call.data)}")
#     # Serialize function name: UTF-8 encoded
#     function_bytes = call.function.encode("utf-8")

#     # Serialize returns_functions: list of (name, source) tuples
#     returns_functions_bytes = b"".join(
#         [
#             name.encode("utf-8") + b"\x00" + _get_function_source(fn).encode("utf-8") + b"\x00"
#             for name, fn in call.returns
#         ]
#     )

#     # Combine all parts with clear separators or fixed lengths
#     serialized = (
#         len(target_bytes).to_bytes(4, "big")
#         + target_bytes
#         + len(data_bytes).to_bytes(4, "big")
#         + data_bytes
#         + len(function_bytes).to_bytes(4, "big")
#         + function_bytes
#         + len(returns_functions_bytes).to_bytes(4, "big")
#         + returns_functions_bytes
#     )

#     return serialized




# def _serialize_multicall(multicall: Multicall) -> bytes:
#     """
#     Serialize a Multicall object into bytes.
#     """

#     parts = []

#     # Serialize block_id: 8 bytes (big endian)
#     parts.append(multicall.block_id.to_bytes(8, "big"))

#     # Serialize chainid: 8 bytes (big endian)
#     parts.append(multicall.chainid.to_bytes(8, "big"))

#     # Serialize require_success: 1 byte (0x01 for True, 0x00 for False)
#     parts.append(b"\x01" if multicall.require_success else b"\x00")

#     # Serialize multicall_address: 20 bytes (assuming '0x' prefix)
#     multicall_address_bytes = bytes.fromhex(multicall.multicall_address.lower()[2:])
#     parts.append(multicall_address_bytes)

#     # Serialize each Call object
#     for call in multicall.calls:
#         call_bytes = _serialize_call(call)
#         parts.append(call_bytes)

#     # Combine all parts into a single byte sequence
#     serialized_multicall = b"".join(parts)
#     return serialized_multicall



# def multicall_to_db_hash(multicall: Multicall) -> str:
#     """
#     Generate a SHA-256 hash for a given Multicall object.
#     """
#     # Serialize the Multicall object into bytes
#     serialized_data = _serialize_multicall(multicall)

#     # Compute the SHA-256 hash of the serialized bytes
#     sha256_hash = hashlib.sha256(serialized_data).hexdigest()

#     return sha256_hash


# def multicall_to_db_hash_parallel(multicalls: list[Multicall], max_workers: int = None) -> list[str]:
#     """
#     Generate SHA-256 hashes for a list of Multicall objects in parallel.

#     Parameters:
#         multicalls (List[Multicall]): A list of Multicall objects to hash.
#         max_workers (int, optional): The maximum number of worker processes to use.
#                                      Defaults to the number of processors on the machine.

#     Returns:
#         List[str]: A list of SHA-256 hash strings corresponding to the input Multicall objects.
#     """
#     with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
#         hashes = list(executor.map(multicall_to_db_hash, multicalls))
#     return hashes



# def identity_with_bool_success(success, value):
#     if success:
#         return value
#     return None


# def make_a_bunch_of_small_multicalls(n_blocks: int, num_extra_calls: int = 10) -> list[Multicall]:
#     """
#     Generate multiple Multicall objects, each containing a set of Call objects with unique function identifiers.

#     Each Multicall includes:
#         - get_timestamp_call
#         - get_block_call
#         - num_extra_calls additional calls with unique 'function' identifiers ("1", "2", ..., "num_extra_calls")

#     Parameters:
#         n_blocks (int): Number of blocks for which to generate Multicall objects.
#         num_extra_calls (int): Number of additional Call objects with unique function identifiers per Multicall.

#     Returns:
#         List[Multicall]: A list of generated Multicall objects.
#     """
#     base_multicall_address = MULTICALL_V3.eth  # Assuming 'eth' is the desired chain

#     # Define base calls for timestamp and block number
#     get_timestamp_call = Call(
#         base_multicall_address,
#         "getCurrentBlockTimestamp()(uint256)",
#         [("timestamp", identity_with_bool_success)],
#     )
#     get_block_call = Call(
#         base_multicall_address,
#         "getBlockNumber()(uint256)",
#         [("block", identity_with_bool_success)],
#     )

#     # Generate additional calls with unique 'function' identifiers ("1", "2", ..., "num_extra_calls")
#     # Replace "getSomeData()(uint256)" with actual function signatures as needed
#     additional_calls = [
#         Call(
#             base_multicall_address,
#             "getSomeData()(uint256)",  # Example function signature; replace as necessary
#             [(str(i), identity_with_bool_success)]
#         )
#         for i in range(1, num_extra_calls + 1)
#     ]

#     # Combine base calls with additional unique calls
#     all_calls = [get_timestamp_call, get_block_call] + additional_calls

#     # Define the range of blocks for which to create Multicall objects
#     start_block = 10_000_000
#     blocks = list(range(start_block, start_block + n_blocks))

#     # Create Multicall objects for each block
#     all_multicalls = [
#         Multicall(
#             calls=all_calls,
#             block_id=block,
#             _w3=BASE_CHAIN.client,
#             require_success=False,
#         )
#         for block in blocks
#     ]
#     return all_multicalls

# def multicall_to_db_hash_serial(multicalls: list[Multicall]) -> list[str]:
#     """
#     Generate SHA-256 hashes for a list of Multicall objects serially using list comprehension.

#     Parameters:
#         multicalls (List[Multicall]): A list of Multicall objects to hash.

#     Returns:
#         List[str]: A list of SHA-256 hash strings corresponding to the input Multicall objects.
#     """
#     hashes = [multicall_to_db_hash(mc) for mc in multicalls]
#     return hashes
   
   
# def benchmark_multicall_hashing(n_blocks: int, num_extra_calls: int, max_workers: int):
#     """
#     Benchmark the time taken to generate and hash Multicall objects both serially and in parallel.

#     Parameters:
#         n_blocks (int): Number of blocks for which to generate Multicall objects.
#         num_extra_calls (int): Number of additional Call objects with unique function identifiers per Multicall.
#         max_workers (int): Number of parallel worker processes for hashing.

#     Returns:
#         None
#     """
#     print(f"Benchmarking with {n_blocks} blocks, {num_extra_calls} extra calls per multicall, and {max_workers} workers.")
    
#     # Measure Multicall Generation Time
#     start_time = time.perf_counter()
#     all_multicalls = make_a_bunch_of_small_multicalls(n_blocks=n_blocks, num_extra_calls=num_extra_calls)
#     generation_time = time.perf_counter() - start_time
#     print(f"Generated {n_blocks} multicalls in {generation_time:.4f} seconds.")

#     # Measure Serial Hashing Time
#     start_time = time.perf_counter()
#     hashes_serial = multicall_to_db_hash_serial(all_multicalls)
#     serial_hashing_time = time.perf_counter() - start_time
#     print(f"Serially hashed {n_blocks} multicalls in {serial_hashing_time:.4f} seconds.")
#     print(f"Average serial hashing time per multicall: {serial_hashing_time / n_blocks:.6f} seconds.")

#     # Measure Parallel Hashing Time
#     start_time = time.perf_counter()
#     hashes_parallel = multicall_to_db_hash_parallel(all_multicalls, max_workers=max_workers)
#     parallel_hashing_time = time.perf_counter() - start_time
#     print(f"Parallel hashed {n_blocks} multicalls in {parallel_hashing_time:.4f} seconds.")
#     print(f"Average parallel hashing time per multicall: {parallel_hashing_time / n_blocks:.6f} seconds.")

#     # Verify Hash Uniqueness (Optional)
#     unique_serial_hashes = len(set(hashes_serial))
#     unique_parallel_hashes = len(set(hashes_parallel))
#     print(f"Unique serial hashes generated: {unique_serial_hashes} out of {n_blocks}.")
#     print(f"Unique parallel hashes generated: {unique_parallel_hashes} out of {n_blocks}.")

#     # Optional: Compare Serial and Parallel Hashes
#     # assert hashes_serial == hashes_parallel, "Mismatch between serial and parallel hashing results."

#     print("-" * 60)


# if __name__ == '__main__':
#     # Define benchmark configurations
#     benchmark_configs = [

#         {"n_blocks": 5000, "num_extra_calls": 500, "max_workers": 1},
#         {"n_blocks": 5000, "num_extra_calls": 500, "max_workers": 2},
#         {"n_blocks": 5000, "num_extra_calls": 500, "max_workers": 8},

#     ]
    
    

#     for config in benchmark_configs:
#         benchmark_multicall_hashing(
#             n_blocks=config["n_blocks"],
#             num_extra_calls=config["num_extra_calls"],
#             max_workers=config["max_workers"]
#         )
        
    