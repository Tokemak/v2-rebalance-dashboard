import os
from pathlib import Path
from dotenv import dotenv_values
import toml
from mainnet_launch.constants.secrets import WORKING_DATA_DIR, ROOT_DIR


def create_streamlit_secrets_from_env(
    env_path: str = ".env",
    output_filename: str = "streamlit_config_secrets.toml",
) -> None:
    """
    Reads environment variables from a .env file and writes them to a TOML file
    for Streamlit secrets usage, and creates an empty .env_example file.

    Args:
        env_path (str): Path to the .env file.
        output_filename (str): Name of the TOML file to create in WORKING_DATA_DIR.
    """
    env_file = Path(env_path)
    if not env_file.exists():
        raise FileNotFoundError(f"Could not find .env file at {env_file}")

    # Load the .env key-value pairs
    env_vars = dotenv_values(env_file)
    if not env_vars:
        raise ValueError(f"No environment variables found in {env_file}")

    # Ensure all keys are strings
    env_vars_str = {str(k): v for k, v in env_vars.items() if v is not None}

    # Create Streamlit secrets TOML
    output_dir = Path(WORKING_DATA_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_filename
    with open(output_path, "w") as f:
        toml.dump(env_vars_str, f)
    print(f"✅ Streamlit secrets TOML created at: {output_path.resolve()}")

    # Create empty .env_example in home directory
    example_path = ROOT_DIR / ".env_example"
    with open(example_path, "w") as f:
        for key in env_vars_str.keys():
            f.write(f"{key}=\n")
    print(f"✅ Empty .env_example created at: {example_path.resolve()}")


if __name__ == "__main__":
    create_streamlit_secrets_from_env()
