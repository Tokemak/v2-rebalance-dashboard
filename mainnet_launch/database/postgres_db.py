import os
import asyncio
from sqlalchemy import text
from dotenv import load_dotenv
from urllib.parse import urlparse
from sqlalchemy.ext.asyncio import create_async_engine
import pandas as pd

from mainnet_launch.constants import eth_client, base_client


print(eth_client.eth.get_block(20_000_000).timestamp)
print(base_client.eth.get_block(20_000_000).timestamp)

load_dotenv()

tmpPostgres = urlparse(os.getenv("DEV_LOCAL_DATABASE_URL"))
engine = create_async_engine(
    f"postgresql+asyncpg://{tmpPostgres.username}:{tmpPostgres.password}@{tmpPostgres.hostname}{tmpPostgres.path}?ssl=require",
    echo=True,  # Enable SQL query logging for debugging.
)


async def run_read_only_query(query: str):
    async with engine.connect() as conn:
        result = await conn.execute(text(query))
        rows = await result.fetchall()
        return rows


async def create_table(create_table_query: str):
    async with engine.begin() as conn:
        await conn.execute(text(create_table_query))


async def add_new_rows(table_name: str, data_to_insert_df: pd.DataFrame):
    """
    Inserts new rows into the preexisting table 'my_table'.
    The rows are added using parameterized queries for safety.
    """
    new_rows = data_to_insert_df.to_records()
    # Parameterized INSERT statement.
    insert_query = """
    INSERT INTO my_table (id, name)
    VALUES (:id, :name)
    """
    # Define new rows as a list of dictionaries (each dictionary maps column names to values).
    new_rows = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    # Use engine.begin() to execute within a transaction.
    async with engine.begin() as conn:
        await conn.execute(text(insert_query), new_rows)
        print("New rows inserted into 'my_table'.")


# -----------------------------
# Example async main to test the functions
# -----------------------------
async def main():

    create_block_timestamp_query = """
        CREATE TABLE block_chain_timestamp (
            block_number  BIGINT NOT NULL,
            chain         VARCHAR(50) NOT NULL,
            unix_timestamp BIGINT NOT NULL,
            PRIMARY KEY (chain, block_number)
            );
        """

    await create_table(create_block_timestamp_query)


# Run the main function using asyncio
if __name__ == "__main__":
    asyncio.run(main())
