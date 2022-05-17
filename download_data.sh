TIMEFRAME=4h

for symbol in DOT FTM AVAX SOL ATOM BNB FTT; do
    echo downloading $symbol data...
    python ccxt_downloader.py \
        --symbol $symbol/USDT \
        --destdir ./data \
        --exchange binance \
        --start-date 2021-01-01 \
        --end-date 2022-05-16 \
        --timeframe $TIMEFRAME
done
