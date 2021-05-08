from opportunity_spotter import BinanceAnalyzer
from binance_provider import Factory
from binance_trader import BinanceTrader
from stream_manager import StreamManager
from market_entity import MarketTickEntity
from analyzer_models import PositionStrategy
from constants import BINANCE

if __name__ == '__main__':
    symbol = BINANCE.SYMBOL
    provider = Factory()

    small_window, large_window = 5, 15
    client = provider.client
    historical_data = client.get_historical_klines(symbol=symbol, interval=client.KLINE_INTERVAL_1MINUTE,
                                                   start_str='{} min ago UTC'.format(large_window * 2))
    binance_ticks = list(map(lambda k_line: MarketTickEntity.map_historical_k_line(k_line, symbol), historical_data))

    analyzer = BinanceAnalyzer(entry_strategy=PositionStrategy.MovingAvg, exit_strategy=PositionStrategy.MACD,
                               min_sample_window=30)
    for tick in binance_ticks:
        analyzer.find_opportunity(tick)

    trader = BinanceTrader(provider.trade_executor(symbol), analyzer)
    manager = StreamManager(trader)
    provider.open_kline_connection(processor=lambda event: manager.consume(event), symbol=symbol)
