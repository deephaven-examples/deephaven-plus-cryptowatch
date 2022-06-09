# Set the list of coins, exchanges, and currencies here
coins = ["btc", "eth", "nmc", "doge", "firo", "shib", "safemoon"]
exchanges = ["bitfinex", "kraken", "gemini", "binance"]
currencies = ["usd", "eur"]

# Change this to false if you don't want to write the data to parquet
write_flag = True

cw_col_defs = {
    "Coin": dht.string, "Exchange": dht.string, "Currency": dht.string, \
    "Datetime": dht.DateTime, "Open": dht.double, "High": dht.double, "Low": dht.double, \
    "Close": dht.double, "Volume": dht.double, "QuoteVol": dht.double, "Granularity": dht.string
}

cw_table_writer = DynamicTableWriter(cw_col_defs)

historical_crypto_data = cw_table_writer.table

for exchange in exchanges:
    for coin in coins:
        for currency in currencies:
            thread = threading.Thread(target=pull_cryptowatch_historical_data, \
                args=(exchange, coin, currency, ))
            pull_cryptowatch_historical_data(exchange, coin, currency)

if write_flag:
    write(historical_crypto_data, "/data/CryptoWatch/historical_crypto_data.parquet", compression_codec_name="GZIP")

    for coin in coins:
        for exchange in exchanges:
            for currency in currencies:
                for granularity in cw_frequencies:
                    subprocess.call(f"mkdir -p /data/CryptoWatch/Coin/{coin}/{exchange}/{currency}/{granularity}", shell=True)
    
    crypto_partitioned = historical_crypto_data.partition_by(["Coin", "Exchange", "Currency", "Granularity"])
    tables = crypto_partitioned.constituent_tables
    
    for table in tables:
        coin = table.j_object.getColumnSource("Coin").get(table.j_table.getRowSet().firstRowKey())
        exchange = table.j_object.getColumnSource("Exchange").get(table.j_table.getRowSet().firstRowKey())
        currency = table.j_object.getColumnSource("Currency").get(table.j_table.getRowSet().firstRowKey())
        granularity = table.j_object.getColumnSource("Granularity").get(table.j_table.getRowSet().firstRowKey())
        fname = f"/data/CryptoWatch/Coin/{coin}/{exchange}/{currency}/{granularity}/data.parquet"
        write(table, fname, compression_codec_name="GZIP")