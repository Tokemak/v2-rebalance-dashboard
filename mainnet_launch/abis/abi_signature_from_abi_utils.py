from web3 import Web3


def get_event_keccak_signature(event_signature: str) -> str:
    """
    Keccak-256(Transfer,address,address,uint256) = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef

    that value is used as topic[0] in event logs
    """
    return Web3.keccak(text=event_signature).hex()


def parse_type(param):
    """Recursively parse types, handling nested structs and arrays."""
    if param["type"] == "tuple":
        # For structs, recursively parse each component
        component_types = ",".join(parse_type(c) for c in param["components"])
        return f"({component_types})"
    elif param["type"].endswith("[]"):
        # For arrays, parse the base type and add array notation
        base_type = param["type"][:-2]
        base_param = {"type": base_type, "components": param.get("components", [])}
        return f"{parse_type(base_param)}[]"
    else:
        # Basic types
        return param["type"]


# useful to get the function and event signatures when using multicall.py
def get_function_and_event_signatures_with_returns(abi_json):
    # eg for multicall.py
    # might need to spot check
    signatures = []

    for item in abi_json:
        # Check if the item is a function
        if item.get("type") == "function":
            func_name = item["name"]
            inputs = item.get("inputs", [])
            input_types = ",".join(parse_type(param) for param in inputs)

            outputs = item.get("outputs", [])
            if outputs:
                output_types = ",".join(parse_type(param) for param in outputs)
                signature = f"{func_name}({input_types})({output_types})"
            else:
                signature = f"{func_name}({input_types})"

        elif item.get("type") == "event":
            func_name = item["name"]
            inputs = item.get("inputs", [])
            input_types = ",".join(parse_type(param) for param in inputs)
            signature = f"{func_name}({input_types})"
        else:
            signature = None

        signatures.append(signature)

    return signatures


# see https://medium.com/mycrypto/understanding-event-logs-on-the-ethereum-blockchain-f4ae7ba50378
