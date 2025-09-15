Make sure each of these functions, those that actually render the page

support this function signature
```
def some_risk_metric_function(
    chain: ChainData, base_asset: TokemakAddress, valid_autopools: list[AutopoolConstants]
):
```

That way they can be used in automated testing. They don't have to use all the arguments