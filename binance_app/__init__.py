import json

from model_academic_trade import LongTrade
from trade_executor_client import AcademicTradeExecutor
from market_tick_analyzer import MarketTickConsolidatedOpportunityFinder
from provider import Factory
from market_trader import ProfessionalTrader
from stream_manager import StreamManager
from market_tick import MarketTickEntity
from analyzer_models import PositionStrategy
from constants import BINANCE, KITE
from strategy_config import MACDParams, MovingAvgParams
from util_json import save_csv_and_json_output


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
                                                       min_sample_window=30,
                                                       entry_params=MovingAvgParams.params,
                                                       exit_params=MACDParams.params)
    for tick in binance_ticks:
        analyzer.find_opportunity(tick)
    trader = ProfessionalTrader(factory.binance_trade_executor(symbol), analyzer)
    manager = StreamManager(trader, MarketTickEntity.map_from_binance_kline)
    factory.open_kline_connection(processor=lambda event: manager.consume(event), symbol=symbol)


def __profit_loss_analysis(trades, symbol):
    net_profit = 0
    profitable_trades, loss_making_trades = 0, 0
    only_profit, only_loss = 0, 0
    for trade in trades:
        trade_profit = trade[LongTrade.PNL]
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


def file_analyzer(event_mapper, file_connection, symbol, macd_params=(12, 26, 9)):
    factory = Factory()
    fast, slow, signal = macd_params
    params = {
        MACDParams.FAST: fast,
        MACDParams.SLOW: slow,
        MACDParams.SIGNAL: signal
    }
    analyzer = MarketTickConsolidatedOpportunityFinder(entry_strategy=PositionStrategy.MACD,
                                                       exit_strategy=PositionStrategy.MACD,
                                                       min_sample_window=30, entry_params=params,
                                                       exit_params=params)
    trading_client: AcademicTradeExecutor = factory.analytical_trade_executor(symbol)
    trader = ProfessionalTrader(trading_client, analyzer)
    manager = StreamManager(trader, lambda j_elem: event_mapper(j_elem, symbol), min_event_delay=-1)

    file_connection(processor=lambda event: manager.consume(event), symbol=symbol)

    pnl = []

    def generate_profit_loss(trade_list, trade_type):
        profit_loss = __profit_loss_analysis(trade_list, symbol)
        profit_loss[MACDParams.FAST] = fast
        profit_loss[MACDParams.SLOW] = slow
        profit_loss[MACDParams.SIGNAL] = signal
        profit_loss[LongTrade.TRADE_TYPE] = trade_type
        pnl.append(profit_loss)

    longs, shorts = trading_client.get_all_trades()
    all_trades = []
    all_trades.extend(longs), all_trades.extend(shorts)
    generate_profit_loss(all_trades, "ALL")
    generate_profit_loss(longs, "longs")
    generate_profit_loss(shorts, "shorts")

    return longs, shorts, all_trades, pnl


def analyze_binance_old_data():
    symbol = BINANCE.SYMBOL
    factory = Factory()
    event_mapper = MarketTickEntity.map_file_row
    file_connection = factory.open_file_kline_connection
    fast, slow, signal = (12, 26, 9)
    long, short, all_trades, pnl = file_analyzer(event_mapper, file_connection, symbol,
                                                 macd_params=(fast, slow, signal))
    save_csv_and_json_output(all_trades,
                             BINANCE.DATA_FILE_WRITE_BASE_PATH + "trades_{}_{}_{}".format(fast, slow, signal))
    save_csv_and_json_output(all_trades,
                             BINANCE.DATA_FILE_WRITE_BASE_PATH + "long_trades_{}_{}_{}".format(fast, slow, signal))
    save_csv_and_json_output(all_trades,
                             BINANCE.DATA_FILE_WRITE_BASE_PATH + "short_trades_{}_{}_{}".format(fast, slow, signal))

    with open(BINANCE.DATA_FILE_WRITE_BASE_PATH + "profit_loss_multi.json", 'w') as handle:
        json.dump(pnl, handle, indent=1)


def analyze_kite_old_data():
    all_pnl = []
    for symbol in KITE.SYMBOLS:
        factory = Factory()
        event_mapper = MarketTickEntity.map_from_kite_event
        file_connection = factory.open_file_kite_connection
        fast, slow, signal = (12, 26, 9)
        long, short, all_trades, pnl = file_analyzer(event_mapper, file_connection, symbol,
                                                     macd_params=(fast, slow, signal))
        # save_csv_and_json_output(all_trades,
        #                          KITE.DATA_FILE_WRITE_BASE_PATH + "trades_{}_{}_{}_{}".format(symbol, fast, slow,
        #                                                                                       signal))
        # save_csv_and_json_output(all_trades,
        #                          KITE.DATA_FILE_WRITE_BASE_PATH + "long_trades_{}_{}_{}_{}".format(symbol, fast, slow,
        #                                                                                            signal))
        # save_csv_and_json_output(all_trades,
        #                          KITE.DATA_FILE_WRITE_BASE_PATH + "short_trades_{}_{}_{}_{}".format(symbol, fast, slow,
        #                                                                                             signal))
        #
        # with open(KITE.DATA_FILE_WRITE_BASE_PATH + "profit_loss_multi_{}.json".format(symbol), 'w') as handle:
        #     json.dump(pnl, handle, indent=1)

        all_pnl.extend(pnl)

    save_csv_and_json_output(all_pnl, KITE.DATA_FILE_WRITE_BASE_PATH + "all_pnl_{}".format(KITE.year))


if __name__ == '__main__':
    analyze_kite_old_data()
    # analyze_binance_old_data()
    # live_trade_binance()
