
├── abis




├── app

The main streamlit app and the inital script to fetch data up to the current day. 


├── database
Logic for handling reading and writing events and processed onchain data to disk.


├── data_fetching
Fetches events and onchain data


│   └── rebalance_plans
Holds the solver rebalance plans .jsons

├── pages

One folder for each page of the app. Each folder holds both the logic to fetch the data and render the plots


│   ├── autopool_crm
Autopool Composite Return Out Metric



│   ├── autopool_diagnostics

Many plots describing the autopool


│   ├── autopool_exposure

Current and historical of value in each destination over time

│   ├── destination_diagnostics
│   ├── gas_costs
│   ├── incentive_token_prices
│   ├── key_metrics
│   ├── protocol_level_profit_and_loss
│   ├── __pycache__
│   ├── rebalance_events
│   └── solver_diagnostics
├── __pycache__
└── working_data
    └── end_of_day
