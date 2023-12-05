# Deephaven plus CryptoWatch

This repository contains functionalities for using the [CryptoWatch REST API](https://docs.cryptowat.ch/rest-api/) within Deephaven.

The CryptoWatch REST API allows users to pull historical crypto data for a large number of coins from various exchanges and for various currencies.

This repository pulls data from CryptoWatch into a Deephaven table, and optionally writes it to a nested parquet directory stucture based on partitioning columns in the table.

## Usage

Two scripts should run when you boot up the session:

- The first defines CryptoWatch utilities and pulls the list of available assets (coins) and exchanges.
- The second defines the function used to pull data for a particular pair (coin + currency) on a given exchange.

The third script, which resides in `/data/notebooks/` will pull historical data for a given list of:

- coins
- exchanges
- currencies

Each unique combination of coin/exchange/currency performs a single REST API request from CryptoWatch.  These requests take 0.015 CryptoWatch credits.

To use the code in this repository, open the script `/data/notebooks/Pull_Coins_Currencies_From_Exchanges_CryptoWatch.py`.  Edit the `coins`, `exchanges`, and `currencies` lists to contain any coins, exchanges, and currencies you are interested in.  Additionally, if you wish to write the crypto data to your local machine, set `write_flag` to True (if it isn't already).  Run the script from the console and it will start pulling (and optionally writing) historical crypto data for a large range of [granularities](https://docs.cryptowat.ch/rest-api/markets/ohlc).

To see a list of valid assets and exchanges, type the following:

```python
print(CryptoWatchAssets.symbols)
print(CryptoWatchExchanges.symbols)
```

CryptoWatch may not have data for a particular pair on a given exchange.  In such a case, the code should skip that pair and move onto the next in the list.

## Pulling from CryptoWatch

Every 24 hours, you are allowed 10 CryptoWatch credits for your own personal use without an API key.  Requests range from 0.002 to 0.015 credits per, which means you can perform several hundred to multiple thousands of requests per day.  The requests for a pair on an exchange cost 0.015 credits, while requests for available assets, exchanges, currencies, etc. cost 0.002.

More information on requests can be found [here](https://docs.cryptowat.ch/rest-api/rate-limit).  All code in this repository will report usage metrics and remaining allowance on every function call.

## Fair Use

It generally goes against terms of use to present data from a source as your own.  Deephaven makes no claim to the ownership or validity of the data found on CryptoWatch.  This repository contains no crypto data by default because it is not ours, but rather theirs, which is free to use on an individual basis.

## Updates

This repository is currently in its first version, v0.0.  Below is a list of dates and updates made:

- 06/09/2022: v0.0.0
  - Push all content
  - Functionalities for pulling pairs on exchanges
  - Functionalities for pulling asset and exchange lists
  - Usage reports
- 06/10/2022: v0.0.1
  - Add date to bulk write

## Note

The code in this repository is built for Deephaven Community Core v0.20.0. No guarantee of forwards or backwards compatibility is given.
