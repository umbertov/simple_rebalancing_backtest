import pandas as pd
import bt

ASSETS = ("ATOM", "AVAX", "BNB", "DOT", "FTM", "FTT", "SOL")

START_DATE = "2021-06-01"


class SetCash(bt.Algo):
    def __init__(self, cash):
        self.cash = cash

    def __call__(self, target):
        target.temp["cash"] = self.cash
        return True


def read_ohlc(path, start_date=START_DATE):
    df = pd.read_csv(path)
    df.index = pd.to_datetime(df.Date)
    df = df[START_DATE:]
    return df.Close.resample("4h").agg("mean")


prices_df = {
    ticker: read_ohlc(f"./data/{ticker}-USDT.binance.2020-12-31.2022-05-15.csv")
    for ticker in ASSETS
}

data = pd.DataFrame({t: series for t, series in prices_df.items()})

btc_price = read_ohlc(f"./data/BTC-USDT.binance.2020-12-31.2022-05-15.csv")

# create the strategy
s = bt.Strategy(
    "rebalance daily",
    [
        SetCash(0.8),
        bt.algos.RunDaily(),
        bt.algos.SelectAll(),
        bt.algos.WeighEqually(),
        bt.algos.Rebalance(),
    ],
)

# create a backtest and run it
test = bt.Backtest(s, data)
res = bt.run(test)

axis = res.plot()
axis.plot(btc_price.rebase())
for asset, df in prices_df.items():
    axis.plot(df.rebase(), label=asset)
axis.legend()
axis.figure.show()

print(res.display())
