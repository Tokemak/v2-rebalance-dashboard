import toml
from dotenv import dotenv_values

# 1. Load your .env into a dict
env = dotenv_values(".env")  # this preserves order and comments out missing files gracefully

def _convert(v: str):
    """Try to cast to bool, int, float; otherwise leave as string."""
    low = v.lower()
    if low in {"true", "false"}:
        return low == "true"
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            continue
    return v

# 2. Apply conversions on all values
toml_dict = {k: _convert(v) for k, v in env.items()}

# 3. Write out a TOML file
with open("working_data/config.toml", "w") as f:
    toml.dump(toml_dict, f)

print("Wrote config.toml")
