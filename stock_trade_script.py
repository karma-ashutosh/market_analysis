import traceback
from datetime import timedelta
from queue import Queue

import yaml
from kiteconnect import KiteTicker, KiteConnect

from alerts import Alert
from bse_util import BseUtil, BseAnnouncementCrawler
from constants import TIMESTAMP, LAST_PRICE, EMPTY_KEY, VOLUME, BUY_QUANTITY, SELL_QUANTITY, LAST_TRADE_TIME, \
    KITE_EVENT_DATETIME_OBJ, INSTRUMENT_TOKEN, BASE_DIR
from exit_strategy import ExitStrategy, ExitStrategyFactory
from general_util import setup_logger
from kite_enums import TransactionType
from kite_util import KiteUtil
from postgres_io import PostgresIO
from result_time_provider import BseCrawlerBasedResultTimeProvider, SummaryFileBasedResultTimeProvider
from score_functions import ScoreFunctions, BaseScoreFunctions
from trade_execution import TradeExecutor, DummyTradeExecutor, KiteTradeExecutor

logger = setup_logger("msg_logger", "./app.log")


class PerSecondLatestEventTracker:
    def __init__(self, window_length_in_seconds: int, keys_to_track: list):
        self.__window_length = window_length_in_seconds
        self.__keys_to_track = keys_to_track
        self.__events = Queue(maxsize=window_length_in_seconds)
        self._sum = [0] * len(keys_to_track)

    def get_current_queue_snapshot(self) -> list:
        return list(self.__events.queue)

    def move(self, json_event: dict):
        current_event = self.__get_marshaled_event(json_event)

        queue_as_list = self.__events.queue
        if len(queue_as_list) > 0:
            last_element = queue_as_list[-1]
            seconds_gap = self.__get_seconds_gap_from_last_received_event(last_element, current_event)
            # print(seconds_gap, 'gap')

            if seconds_gap < 1:
                self.__overwrite_event(last_element, current_event)
            else:
                for i in range(seconds_gap - 1):
                    self.__put(last_element)

            self.__put(current_event)
        else:
            self.__put(current_event)

    def __put(self, event):
        if self.__events.qsize() == self.__events.maxsize:
            self.__events.get_nowait()
        self.__events.put_nowait(event)

    @staticmethod
    def __get_seconds_gap_from_last_received_event(last_element, marshaled_event):
        last_dt = last_element[KITE_EVENT_DATETIME_OBJ]
        curr_dt = marshaled_event[KITE_EVENT_DATETIME_OBJ]
        td = curr_dt - last_dt
        seconds_gap = int(td.total_seconds())
        return seconds_gap

    def __overwrite_event(self, target, source):
        # print(self.__keys_to_track)
        for key in self.__keys_to_track:
            # print(key)
            target[key] = source.get(key)

    @staticmethod
    def __round_seconds(date_time_object):
        new_date_time = date_time_object

        if new_date_time.microsecond >= 500000:
            new_date_time = new_date_time + timedelta(seconds=1)

        return new_date_time.replace(microsecond=0)

    def __get_marshaled_event(self, json_event):
        marshaled_event = {}
        self.__overwrite_event(marshaled_event, json_event)

        dt = json_event[TIMESTAMP]
        marshaled_event[KITE_EVENT_DATETIME_OBJ] = self.__round_seconds(dt)

        return marshaled_event


class MarketPosition:
    def __init__(self, exit_strategy_factory: ExitStrategyFactory):
        self._exit_strategy_factory = exit_strategy_factory
        self._entry_scores = None
        self._entry_event = None
        self._exit_event = None
        self._transaction_type = None
        self._took_position = False
        self._trade_completed = False
        self._entry_price = None
        self._exit_strategy: ExitStrategy = None

    @staticmethod
    def __price(event):
        return event[LAST_PRICE]

    def consume_event(self, market_event):
        self._exit_strategy.consume_event(market_event)

    def enter(self, entry_event, transaction_type: TransactionType, scores: list):
        self._entry_event = entry_event
        self._entry_price = self.__price(entry_event)
        self._transaction_type = transaction_type
        self._entry_scores = scores
        self._took_position = True
        self._exit_strategy = self._exit_strategy_factory.exit_strategy(transaction_type, entry_event)
        msg = "buy" if transaction_type == TransactionType.LONG else "sell"
        logger.info("{} stocks at : ".format(msg) + str(entry_event))

    def entry_price(self):
        return self._entry_event[LAST_PRICE]

    def exit(self, exit_event):
        self._exit_event = exit_event
        self._trade_completed = True

    def is_trade_done(self):
        return self._trade_completed

    def should_exit(self):
        return self._exit_strategy.should_exit()

    def enter_transaction_type(self):
        return self._transaction_type

    def get_summary(self):
        return {
            'entry': self._entry_event,
            'exit': self._exit_event,
            'score': self._entry_scores,
            'type': self._transaction_type.description if self._transaction_type else None
        }


class MarketChangeDetector:
    def __init__(self, window_len, score_functions: ScoreFunctions, trading_sym, trade_executor: TradeExecutor,
                 exit_strategy_factory: ExitStrategyFactory):
        self._exit_strategy_factory = exit_strategy_factory
        self._base_filter_func = score_functions.base_filter
        self._long_score_func_list = score_functions.long_score_func_list()
        self._short_score_func_list = score_functions.short_score_func_list()
        self._score_sum_threshold = 4
        self._score_filter_func = lambda score_list: \
            all([score > 0 for score in score_list]) * sum(score_list) > self._score_sum_threshold

        keys_to_track = [EMPTY_KEY, TIMESTAMP, VOLUME, BUY_QUANTITY, SELL_QUANTITY, LAST_TRADE_TIME,
                         LAST_PRICE]
        self._event_window_15_sec = PerSecondLatestEventTracker(window_length_in_seconds=window_len,
                                                                keys_to_track=keys_to_track)

        self._filter_pass_key_name = 'bool'
        self._filter_pass_queue = PerSecondLatestEventTracker(window_length_in_seconds=window_len,
                                                              keys_to_track=[self._filter_pass_key_name])

        self._profit_limit = 0.015
        self._loss_limit = 0.015

        self._position = MarketPosition(self._exit_strategy_factory)

        self._trade_completed = False
        self._trading_sym = trading_sym

        self._trade_executor = trade_executor

    def run(self, market_event):
        if not self._position.is_trade_done():
            if not self._position._took_position:
                self._try_take_position(market_event)
            else:
                self._position.consume_event(market_event)
                self._try_exiting(market_event)

    def _try_exiting(self, market_event):
        if self._position.should_exit() and not self._position.is_trade_done():
            if self._position.enter_transaction_type() == TransactionType.LONG:
                self._trade_executor.execute_trade(self._trading_sym, market_event, TransactionType.SHORT)
                self._position.exit(market_event)
            else:
                self._trade_executor.execute_trade(self._trading_sym, market_event, TransactionType.LONG)
                self._position.exit(market_event)

    def _try_take_position(self, market_event):
        def set_flag_with_base_filter_func():
            if self._base_filter_func(current_event_list_view):
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
            else:
                self._filter_pass_queue.move(self.get_filter_event(market_event, False))

        self._event_window_15_sec.move(market_event)
        current_event_list_view = self._event_window_15_sec.get_current_queue_snapshot()
        if any([e[self._filter_pass_key_name] for e in self._filter_pass_queue.get_current_queue_snapshot()]):
            long_scores = list(map(lambda func: func(current_event_list_view), self._long_score_func_list))
            short_scores = list(map(lambda func: func(current_event_list_view), self._short_score_func_list))
            if self._score_filter_func(long_scores):
                self._trade_executor.execute_trade(self._trading_sym, market_event, TransactionType.LONG)
                self._position.enter(market_event, TransactionType.LONG, long_scores)
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
            elif self._score_filter_func(short_scores):
                self._trade_executor.execute_trade(self._trading_sym, market_event, TransactionType.SHORT)
                self._position.enter(market_event, TransactionType.SHORT, short_scores)
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
            else:
                set_flag_with_base_filter_func()

        else:
            set_flag_with_base_filter_func()

    @staticmethod
    def debug_point(current_event_list_view):
        if current_event_list_view[-1][KITE_EVENT_DATETIME_OBJ].hour == 14 \
                and current_event_list_view[-1][KITE_EVENT_DATETIME_OBJ].minute == 54 \
                and current_event_list_view[-1][KITE_EVENT_DATETIME_OBJ].second == 7:
            x = 0

    def get_filter_event(self, market_event, state: bool):
        filter_event = {
            TIMESTAMP: market_event.get(TIMESTAMP),
            self._filter_pass_key_name: state
        }
        return filter_event

    def get_summary(self):
        return self._position.get_summary()


class MainClass:
    def __init__(self, simulation=True):
        with open('./config.yml') as handle:
            config = yaml.load(handle)
        self._postgres = PostgresIO(config['postgres-config'])
        self._postgres.connect()

        self._bse = BseUtil(config, self._postgres)
        self._bse_announcement_crawler = BseAnnouncementCrawler(self._postgres, config)
        self._k_util = KiteUtil(self._postgres, config)

        self._market_stats = self._bse.get_all_stats()
        self._market_change_detector_dict = {}

        self._exit_strategy_factory = ExitStrategyFactory(config['exit_config'])

        self._instruments_to_fetch = self._get_instruments_to_fetch()
        self._instruments_to_ignore = set()
        if not simulation:
            self.use_kite_trade_executor()
            self._result_time_provider = BseCrawlerBasedResultTimeProvider(BseAnnouncementCrawler(self._postgres, config))
        else:
            self._trade_executor = DummyTradeExecutor()
            self._result_time_provider = SummaryFileBasedResultTimeProvider(BASE_DIR + "/summary.csv")

        self.__alert = Alert(config)

    def use_kite_trade_executor(self):
        session_info = self._k_util.get_current_session_info()['result'][0]
        kite_connect = KiteConnect(session_info['api_key'], session_info['access_token'])
        self._trade_executor = KiteTradeExecutor(kite_connect)

    def _volume_median_for_instrument_code(self, trading_sym):
        stat = list(filter(lambda j: j['symbol'] == trading_sym, self._market_stats))[0]
        return float(stat['volume_median'])

    def _get_market_change_detector(self, tick) -> MarketChangeDetector:
        def _create_market_change_detector():
            score_func = BaseScoreFunctions(self._volume_median_for_instrument_code(trading_sym), 0.2, security_code,
                                            self._result_time_provider)
            return MarketChangeDetector(15, score_func, trading_sym, self._trade_executor, self._exit_strategy_factory)

        instrument_code = tick[INSTRUMENT_TOKEN]
        trading_sym, security_code = self._k_util.map_instrument_ids_to_trading_symbol_security_code(instrument_code)

        if instrument_code not in self._market_change_detector_dict.keys():
            self._market_change_detector_dict[instrument_code] = _create_market_change_detector()

        return self._market_change_detector_dict[instrument_code]

    def _get_instruments_to_fetch(self):
        results_for_today = self._bse.get_result_announcement_meta_for_today()
        results_for_yesterday = self._bse.get_result_announcement_meta_for_yesterday()
        results_for_today.extend(results_for_yesterday)

        security_codes = list(map(lambda j: j['security_code'], results_for_today))
        instrument_mapping = self._k_util.map_bse_code_to_instrument_id(security_codes)
        return [int(v) for v in instrument_mapping.values()]

    def run_with_kite_stream(self):
        if not isinstance(self._trade_executor, KiteTradeExecutor):
            raise Exception("To run with kite stream, trade executor has to be of type KiteTradeExecutor")

        def on_ticks(ws, ticks):
            # Callback to receive ticks.
            self.__alert.send_heartbeat("stock_trade_script")
            self.handle_ticks_safely(ticks)
            # for tick in ticks:
            #     self._get_market_change_detector(str(tick[INSTRUMENT_TOKEN])).run(tick)
            # for key in tick.keys():
            #     if isinstance(tick[key], datetime):
            #         tick[key] = str(tick[key])

            # stock_logger.info("{}".format(json.dumps(ticks)))

        def on_connect(ws, response):
            # Callback on successful connect.
            # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).

            ws.subscribe(self._instruments_to_fetch)

            # Set RELIANCE to tick in `full` mode.
            ws.set_mode(ws.MODE_FULL, self._instruments_to_fetch)

        def on_close(ws, code, reason):
            # On connection close stop the main loop
            # Reconnection will not happen after executing `ws.stop()`
            ws.stop()

        session_info = self._k_util.get_current_session_info()['result'][0]
        kws = KiteTicker(session_info['api_key'], session_info['access_token'])
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.on_close = on_close

        kws.connect()

    def handle_ticks_safely(self, ticks):
        for index in range(len(ticks)):
            try:
                if ticks[index][INSTRUMENT_TOKEN] not in self._instruments_to_ignore:
                    self._get_market_change_detector(ticks[index]).run(ticks[index])
            except Exception as e:
                traceback.print_exc()
                logger.error("ignoring instrument at instrument_token: {}".format(ticks[index][INSTRUMENT_TOKEN]))
                self._instruments_to_ignore.add(ticks[index][INSTRUMENT_TOKEN])

    def get_summary(self):
        summaries = {}
        for key in self._market_change_detector_dict.keys():
            mcd = self._market_change_detector_dict[key]
            summaries[key] = mcd.get_summary()
        return summaries


if __name__ == '__main__':
    MainClass().run_with_kite_stream()
