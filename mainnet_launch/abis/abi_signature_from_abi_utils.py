import json


# These work
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


def get_function_signatures_with_returns(abi_json):
    # eg for multicall.py
    # might need to spot check
    signatures = []

    for item in abi_json:
        # Check if the item is a function
        if item.get("type") == "function":
            func_name = item["name"]

            # Parse input types
            inputs = item.get("inputs", [])
            input_types = ",".join(parse_type(param) for param in inputs)

            # Parse output types
            outputs = item.get("outputs", [])
            if outputs:
                output_types = ",".join(parse_type(param) for param in outputs)
                signature = f"{func_name}({input_types})({output_types})"
            else:
                signature = f"{func_name}({input_types})"

            signatures.append(signature)

    return signatures
