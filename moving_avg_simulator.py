from moving_avg_simulator_utils import MovingAvgTradeSimulator, TradeSimulator, DataSeriesProvider


class StockPnLAnalyzer:
    def __init__(self, symbol, provider: DataSeriesProvider, smaller_window=5, larger_window=15, debug=False):
        self.__symbol = symbol
        self.__debug = debug
        self.__larger_window = larger_window
        self.__smaller_window = smaller_window
        self.trades = None
        self.summary = None
        self.data_series_provider = provider

    def analyze(self):
        self.trades = self.__simulated_trades()
        self.summary = self.__profit_loss_analysis()
        return self

    def __simulated_trades(self):
        simulator = MovingAvgTradeSimulator(self.data_series_provider, self.__smaller_window, self.__larger_window)
        cross_overs = simulator.get_cross_overs()
        trade_simulator = TradeSimulator(cross_overs, 10000, self.__symbol)
        trades = trade_simulator.execute_trades()
        return trades

    def __profit_loss_analysis(self):
        net_profit = 0
        profitable_trades, loss_making_trades = 0, 0
        only_profit, only_loss = 0, 0
        for trade in self.trades:
            trade_profit = trade.total_profit
            net_profit = net_profit + trade_profit
            if trade_profit > 0:
                only_profit = only_profit + trade_profit
                profitable_trades = profitable_trades + 1
            else:
                only_loss = loss_making_trades - trade_profit
                loss_making_trades = loss_making_trades + 1
        result = {
            "symbol": self.__symbol,
            "net_profit": int(net_profit),
            "profitable_trades": profitable_trades,
            "only_profit": int(only_profit),
            "loss_making_trades": loss_making_trades,
            "only_loss": int(only_loss)
        }
        return result


class CombinedCrossOverGenerator:
    def __init__(self, symbol, provider: DataSeriesProvider, small_window=5, large_window=15):
        self.symbol = symbol
        self.small_window = small_window
        self.large_window = large_window
        self.simulator = MovingAvgTradeSimulator(provider, small_window, large_window)

    def get_cross_over_jarr(self):
        cross_overs = self.simulator.get_cross_overs()
        for cross_over in cross_overs:
            cross_over.symbol = self.symbol
        return cross_overs

