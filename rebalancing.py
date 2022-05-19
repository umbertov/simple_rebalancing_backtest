import pandas as pd
import bt

ASSETS = (
    "BTC",
    "ETH",
    "MATIC",
    "XMR",
    "ALGO",
    "ATOM",
    "AVAX",
    "BNB",
    "DOT",
    "FTM",
    "FTT",
    "SOL",
)

START_DATE = "2021-05-01"

CASH_PCT = 10 / 100  # unused, see SetCash docstring


class SetCash(bt.Algo):
    """unused, bc RunIfOutOfBounds breaks when temp['cash'] is set"""

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


def rebalance_oob(tolerance):
    return bt.Strategy(
        f"Rebalance {tolerance*100:.2f}% oob",
        [
            bt.algos.SelectAll(),
            bt.algos.WeighEqually(),
            bt.algos.Or([bt.algos.RunOnce(), bt.algos.RunIfOutOfBounds(tolerance)]),
            bt.algos.Rebalance(),
        ],
    )


s_daily = bt.Strategy(
    "Rebalance daily",
    [
        bt.algos.RunDaily(),
        bt.algos.SelectAll(),
        bt.algos.WeighEqually(),
        bt.algos.Rebalance(),
    ],
)

s_monthly = bt.Strategy(
    "Rebalance monthly",
    [
        bt.algos.RunMonthly(),
        bt.algos.SelectAll(),
        bt.algos.WeighEqually(),
        bt.algos.Rebalance(),
    ],
)


def backtest(
    s, data, show_plot=False, return_plot=False, plot_assets=False, print_res=False
):
    # create a backtest and run it
    test = bt.Backtest(s, data)
    res = bt.run(test)

    axis = res.plot("1d")
    if plot_assets:
        for asset, series in data.iteritems():
            axis.plot(series.rebase(), label=asset)
    axis.legend()
    if show_plot:
        axis.figure.show()
    if print_res:
        print(res.display())
    if return_plot:
        return res, axis
    return res


results = {
    s.name: backtest(s, data)
    for s in (
        rebalance_oob(5 / 100),
        rebalance_oob(10 / 100),
        rebalance_oob(30 / 100),
        rebalance_oob(60 / 100),
        s_monthly,
        s_daily,
    )
}
equities = pd.DataFrame({strat: res.prices.squeeze() for strat, res in results.items()})
ax = equities.plot()
ax.figure.show()
