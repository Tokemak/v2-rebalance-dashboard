# File Structure


mainnet_launch/

├── abis

Holds ABI jsons, constants and helper methods for using abi function signatures with mulitcall.py

├── app

The main app of the dashboard. Has configuation details and a startup script.

├── constants.py

Onchain addresses and app relatated constants (paths, api keys, etc)


├── database

The .db files themselves and methods to read and write processed event and onchain data using sqlite


├── data_fetching

Methods to quickly fetch contract events and onchain function calls.


├── destinations.py

Defines what a "Destination" is and how to get all destinations deployed since launch. This is relevant because one set of Dex / Staking contract can be used by multiple Autopools. Also destinations are occasioanlly updated and this lets them be stiched together on the UI. Eg if a destination calculator is upgraded we still want to think of that as the same destination even though one of the contracts is different. 


├── notebooks

Work in progress jupyter notebooks and notes

├── pages

Each subfolder here is a seperate tab. In general I'm trying to keep the logic for each tab in a seperate folder. However there is still some overlap

For example:

The Destination Diagnostics tab uses data from the `getDestinationSummaryStats()` call but that data is fetched and stored in the Autopool Diagnostics tab because it is also used by charts in that tab. 


tests/

test_pages.py

Go though each page and autopool in the UI and ensures that it can run without error.Run this with `$ poetry run test-pages`


test_api_keys.py

Minimal tests that make sure that the secrets in .env are valid

