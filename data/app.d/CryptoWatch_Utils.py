from deephaven import parquet as dhpq
from deephaven.pandas import to_table, to_pandas
import pandas as pd
import numpy as np
import requests

# List all assets
def pull_cryptowatch_assets():
    resp = requests.get("https://api.cryptowat.ch/assets")
    json_data = resp.json()

    allowance = json_data["allowance"]
    cost = allowance["cost"]
    remaining = allowance["remaining"]
    print(f"Used {cost} CryptoWatch credits, {remaining} remaining.")

    result = json_data["result"]

    class CryptoWatchAssets:
        pass

    CWAssets = CryptoWatchAssets()

    CWAssets.ids = [item["id"] for item in result]
    CWAssets.symbols = [item["symbol"] for item in result]
    CWAssets.names = [item["name"] for item in result]
    CWAssets.fiats = [item["fiat"] for item in result]
    CWAssets.routes = [item["route"] for item in result]

    return CWAssets

def write_cryptowatch_assets(CWAssets):
    df_assets = pd.DataFrame({\
        "IDs": CWAssets.ids,\
        "Symbols": CWAssets.symbols,\
        "Names": CWAssets.names,\
        "IsFIAT": CWAssets.fiats,\
        "Routes": CWAssets.routes\
    })
    assets_table = to_table(df_assets)
    df_assets = None
    dhpq.write(assets_table, "/data/CryptoWatch/Utils/Assets.parquet", compression_codec_name="GZIP")

def read_cryptowatch_assets():
    df_assets = to_pandas(dhpq.read("/data/CryptoWatch/Utils/Assets.parquet"))

    class CryptoWatchAssets:
        pass
    
    CWAssets = CryptoWatchAssets()

    CWAssets.ids = df_assets["IDs"]
    CWAssets.symbols = df_assets["Symbols"]
    CWAssets.names = df_assets["Names"]
    CWAssets.fiats = df_assets["IsFIAT"]
    CWAssets.routes = df_assets["Routes"]

    return CWAssets

# List all exchanges
def pull_cryptowatch_exchanges():
    resp = requests.get("https://api.cryptowat.ch/exchanges")
    json_data = resp.json()

    allowance = json_data["allowance"]
    cost = allowance["cost"]
    remaining = allowance["remaining"]
    print(f"Used {cost} CryptoWatch credits, {remaining} remaining.")

    result = json_data["result"]

    class CryptoWatchExchanges:
        pass

    CWExchanges = CryptoWatchExchanges()

    CWExchanges.ids = [item["id"] for item in result]
    CWExchanges.symbols = [item["symbol"] for item in result]
    CWExchanges.names = [item["name"] for item in result]
    CWExchanges.routes = [item["route"] for item in result]
    CWExchanges.actives = [item["active"] for item in result]

    return CWExchanges

def write_cryptowatch_exchanges(CWExchanges):
    df_exchanges = pd.DataFrame({\
        "IDs": CWExchanges.ids,\
        "Symbols": CWExchanges.symbols,\
        "Names": CWExchanges.names,\
        "Routes": CWExchanges.routes,\
        "IsActive": CWExchanges.actives\
    })
    exchanges_table = to_table(df_exchanges)
    dhpq.write(exchanges_table, "/data/CryptoWatch/Utils/Exchanges.parquet", compression_codec_name="GZIP")

def read_cryptowatch_assets():
    df_exchanges = to_pandas(dhpq.read("/data/CryptoWatch/Utils/Exchanges.parquet"))

    class CryptoWatchExchanges:
        pass
    
    CWExchanges = CryptoWatchExchanges()

    CWExchanges.ids = df_exchanges["IDs"]
    CWExchanges.symbols = df_exchanges["Symbols"]
    CWExchanges.names = df_exchanges["Names"]
    CWExchanges.routes = df_exchanges["Routes"]
    CWExchanges.actives = df_exchanges["IsActive"]

    return CWExchanges

def report_usage_metrics(allowance):
    cost = allowance["cost"]
    remaining = allowance["remaining"]
    print(f"Used {cost} CryptoWatch credits, {remaining} remaining.")

CryptoWatchExchanges = pull_cryptowatch_exchanges()
CryptoWatchAssets = pull_cryptowatch_assets()
cw_frequencies = ["1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "3d", "1w", "1w_Monday"]
cw_freq_vals = ["60", "180", "300", "900", "1800", "3600", "7200", "14400", "21600", "43200", "86400", "259200", "604800", "604800_Monday"]