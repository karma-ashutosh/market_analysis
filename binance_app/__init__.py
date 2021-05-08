import json

from trade_executor_client import AcademicTradeExecutor
from market_tick_analyzer import MarketTickConsolidatedOpportunityFinder
from provider import Factory
from market_trader import ProfessionalTrader
from stream_manager import StreamManager
from market_tick import MarketTickEntity
from analyzer_models import PositionStrategy
from constants import BINANCE


def live_trade_binance():
    symbol = BINANCE.SYMBOL
    factory = Factory()
    small_window, large_window = 5, 15
    client = factory.client
    historical_data = client.get_historical_klines(symbol=symbol, interval=client.KLINE_INTERVAL_1MINUTE,
                                                   start_str='{} min ago UTC'.format(large_window * 2))
    binance_ticks = list(map(lambda k_line: MarketTickEntity.map_historical_k_line(k_line, symbol), historical_data))
    analyzer = MarketTickConsolidatedOpportunityFinder(entry_strategy=PositionStrategy.MovingAvg,
                                                       exit_strategy=PositionStrategy.MACD,
                                                       min_sample_window=30)
    for tick in binance_ticks:
        analyzer.find_opportunity(tick)
    trader = ProfessionalTrader(factory.binance_trade_executor(symbol), analyzer)
    manager = StreamManager(trader, MarketTickEntity.map_from_binance_kline)
    factory.open_kline_connection(processor=lambda event: manager.consume(event), symbol=symbol)


def analyze_old_data():

    def __profit_loss_analysis():
        net_profit = 0
        profitable_trades, loss_making_trades = 0, 0
        only_profit, only_loss = 0, 0
        for trade in trades:
            trade_profit = trade[AcademicTradeExecutor.Trade.PNL]
            net_profit = net_profit + trade_profit
            if trade_profit > 0:
                only_profit = only_profit + trade_profit
                profitable_trades = profitable_trades + 1
            else:
                only_loss = only_loss - trade_profit
                loss_making_trades = loss_making_trades + 1
        result = {
            "symbol": symbol,
            "net_profit": net_profit,
            "profitable_trades": profitable_trades,
            "only_profit": only_profit,
            "loss_making_trades": loss_making_trades,
            "only_loss": only_loss
        }
        return result

    symbol = BINANCE.SYMBOL
    factory = Factory()
    analyzer = MarketTickConsolidatedOpportunityFinder(entry_strategy=PositionStrategy.MovingAvg,
                                                       exit_strategy=PositionStrategy.MACD,
                                                       min_sample_window=30)
    trading_client: AcademicTradeExecutor = factory.analytical_trade_executor(symbol)
    trader = ProfessionalTrader(trading_client, analyzer)
    manager = StreamManager(trader, lambda j_elem: MarketTickEntity.map_file_row(j_elem, symbol), min_event_delay=-1)
    factory.open_file_kline_connection(processor=lambda event: manager.consume(event), symbol=symbol)

    trades = trading_client.get_all_trades()
    with open("/tmp/trades.json", 'w') as handle:
        json.dump(trades, handle, indent=1)

    profit_loss = __profit_loss_analysis()
    with open("/tmp/profit_loss.json", 'w') as handle:
        json.dump(profit_loss, handle, indent=1)


if __name__ == '__main__':
    analyze_old_data()
    # live_trade_binance()
