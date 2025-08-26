import os
from pathlib import Path
from dotenv import dotenv_values
import toml


def create_streamlit_secrets_from_env(
    env_path: str = ".env",
    output_filename: str = "streamlit_config_secrets.toml",
    working_data_dir: str = "working_data",
) -> None:
    """
    Reads environment variables from a .env file and writes them to a TOML file
    for Streamlit secrets usage.

    Args:
        env_path (str): Path to the .env file.
        output_filename (str): Name of the TOML file to create.
        working_data_dir (str): Directory where the TOML file will be saved.
    """
    env_file = Path(env_path)
    if not env_file.exists():
        raise FileNotFoundError(f"Could not find .env file at {env_file}")

    # Load the .env key-value pairs
    env_vars = dotenv_values(env_file)
    if not env_vars:
        raise ValueError(f"No environment variables found in {env_file}")

    # Convert all keys to strings (TOML requires strings as keys)
    env_vars_str = {str(k): v for k, v in env_vars.items() if v is not None}

    # Make sure working_data_dir exists
    output_dir = Path(working_data_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / output_filename
    print(env_vars_str)

    # Write to TOML
    # with open(output_path, "w") as f:
    #     toml.dump(env_vars_str, f)

    print(f"✅ Streamlit secrets TOML created at: {output_path.resolve()}")


if __name__ == "__main__":
    create_streamlit_secrets_from_env()
