# DEPRECATED

# Database Notes


## General Pattern

```NAV_PER_SHARE_TABLE = "NAV_PER_SHARE_TABLE"

def add_new_nav_per_share_to_table():
    if should_update_table(NAV_PER_SHARE_TABLE):
        for chain in ALL_CHAINS:
            highest_block_already_fetched = get_earliest_block_from_table_with_chain(NAV_PER_SHARE_TABLE, chain)
            blocks = [b for b in build_blocks_to_use(chain) if b >= highest_block_already_fetched]
            nav_per_share_df = _fetch_nav_per_share_from_external_source(chain, blocks)
            write_dataframe_to_table(nav_per_share_df, NAV_PER_SHARE_TABLE)
```


`should_update_table(NAV_PER_SHARE_TABLE)`

returns True if it’s been longer than SHOULD_UPDATE_DATABASE_MAX_LATENCY since the last update.


`for chain in ALL_CHAINS:`
Loops through each chain or autopool and fetches the needed data. 


`highest_block_already_fetched = get_earliest_block_from_table_with_chain(NAV_PER_SHARE_TABLE, chain)`

Returns either the chain’s deployment block or the highest block already in the table. This is to prevent fetching duplicate data.


`blocks = [b for b in build_blocks_to_use(chain) if b >= highest_block_already_fetched]`


`build_blocks_to_use(chain)` defines the blocks to use for each onchain call accross the app.
Only fetch data for new blocks.


`nav_per_share_df = _fetch_nav_per_share_from_external_source(chain, blocks)`
Retrieves and cleans new on-chain data for those blocks, this can involve fetching events and onchain calls.


`write_dataframe_to_table(nav_per_share_df, NAV_PER_SHARE_TABLE)`

write_dataframe_to_table creates or appends rows to NAV_PER_SHARE_TABLE, ensuring no duplicates are introduced.


Whenever this table is used, `add_new_nav_per_share_to_table()` is called first to refresh the data.


Data displayed on the app is always first read from disk.

## Wide and Long Tables

### Wide Tables

What I'm calling a "wide table" is a table that does not require any pivots to be used.

ie each row is an instance and each column is an attribute.

For example in the `DESTINATION_DETAILS_TABLE`

48,DESTINATION_DETAILS_TABLE,vaultAddress,TEXT,101
49,DESTINATION_DETAILS_TABLE,exchangeName,TEXT,101
50,DESTINATION_DETAILS_TABLE,dexPool,TEXT,101
51,DESTINATION_DETAILS_TABLE,lpTokenAddress,TEXT,101
52,DESTINATION_DETAILS_TABLE,lpTokenSymbol,TEXT,101
53,DESTINATION_DETAILS_TABLE,lpTokenName,TEXT,101
54,DESTINATION_DETAILS_TABLE,autopool,TEXT,101
55,DESTINATION_DETAILS_TABLE,vault_name,TEXT,101

Each row is a unique destination for an autopool


When a new destination is upgraded or deployed then a row is added to this table. I don't expect any columns to be added to this table. 

### Long Tables

A "long table" is a table that does require pivots to be used.

for example, 

On way of storing nav per share would be to 

block, autoETH, autoLRT, balETH, baseETH
5000, 1.1, 1.2, 1.02, 1.01

where each row is a point in time and each column is an autopool. However, we expect to add more autopools, so we would need to add new columns and then backfill with onchain calls the new rows for the table. 


Instead it is stored in a long format:

block	autopool	nav_per_share
5000	autoETH	1.1
5000	autoLRT	1.2
5000	balETH	1.02
5000	baseETH	1.01

That way rows are never changed or deleted but instead only added or read. 

Then it is converted back to the wide format with `pd.DataFrame.pivot`.

## Schema

In general this does not define an explcit schema for each table but instead takes the final pd.DataFrame used by the plots and saves that to disk.

You can see the details (colums, datatypes, number of rows) of each table by calling `mainnet_launch.database.database_operations.get_all_tables_info()`


### Should Update

`TABLE_NAME_TO_LAST_UPDATED`

This contains the table name and the last unix timestamp rows were added.


    table_name TEXT PRIMARY KEY
    last_updated_unix_timestamp INTEGER


### Block, Chain  and Timestamp

    timestamp TIMESTAMP
    block INTEGER
    chain TEXT

### Transaction Hash, Gas Used, Gas Price

    hash TEXT PRIMARY KEY
    gas_price INTEGER
    gas_used INTEGER

Stores the gas price and gas used by a transaction by hash.

gasCostInETH = gas_used * gas_price / 1e18 