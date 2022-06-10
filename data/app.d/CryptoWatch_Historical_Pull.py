from deephaven import DynamicTableWriter
from deephaven.parquet import write
from deephaven import pandas as dhp
from deephaven import dtypes as dht
from deephaven import time as dtu

import subprocess
import threading
import datetime
import os

def datetime_from_seconds(seconds):
    return dtu.millis_to_datetime(seconds * 1000)

def pull_cryptowatch_historical_data(exchange, coin, currency):
    global CryptoWatchExchanges, CryptoWatchAssets, cw_frequencies, cw_freq_vals

    if exchange not in CryptoWatchExchanges.symbols:
        print(f"The exchange {exchange} is not supported by CryptoWatch.")
        sys.exit()
    if coin not in CryptoWatchAssets.symbols:
        print(f"The coin {coin} is not supported by CryptoWatch.")

    url = f"https://api.cryptowat.ch/markets/{exchange}/{coin}{currency}/ohlc"
    resp = requests.get(url)

    if not(resp.status_code == 200):
        print(f"Request for {coin}-{currency} on {exchange} failed.  Continuing to the next one...")
        return

    json_data = resp.json()

    report_usage_metrics(json_data["allowance"])

    try:
        result = json_data["result"]
    except KeyError:
        print(f"No data found for {coin}-{currency} on {exchange}.  Moving onto the next one...")
        return

    print(f"Pulling data for {coin}-{currency} on {exchange}.")

    for freq_val in result:
        freq = cw_frequencies[cw_freq_vals.index(freq_val)]
        fname = f"{coin}_{currency}_{exchange}_{freq}.parquet"
        data = result[freq_val]
        for ohlc in data:
            dt = datetime_from_seconds(ohlc[0])
            open = ohlc[1]
            high = ohlc[2]
            low = ohlc[3]
            close = ohlc[4]
            volume = ohlc[5]
            quote_vol = ohlc[6]

            cw_table_writer.write_row(coin, exchange, currency, dt, open, high, low, close, volume, quote_vol, freq)
