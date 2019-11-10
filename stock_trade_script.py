import json
from datetime import datetime
from datetime import timedelta
from enum import Enum
from queue import Queue
import traceback

import yaml
from kiteconnect import KiteTicker, KiteConnect

from bse_util import BseUtil, BseAnnouncementCrawler
from general_util import csv_file_with_headers_to_json_arr, json_arr_to_csv, flatten
from general_util import setup_logger
from kite_util import KiteUtil
from postgres_io import PostgresIO
from kite_trade import place_order
from kite_enums import Variety, Exchange, PRODUCT, OrderType, VALIDITY

stock_logger = setup_logger("stock_logger", "/data/kite_websocket_data/stock.log", msg_only=True)
logger = setup_logger("msg_logger", "./app.log")

EMPTY_KEY = ''

TIMESTAMP = 'timestamp'

LAST_PRICE = 'last_price'

LAST_TRADE_TIME = 'last_trade_time'

SELL_QUANTITY = 'sell_quantity'

BUY_QUANTITY = 'buy_quantity'

VOLUME = 'volume'


class PerSecondLatestEventTracker:
    DATETIME_OBJ = 'datetime'

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
        last_dt = last_element[PerSecondLatestEventTracker.DATETIME_OBJ]
        curr_dt = marshaled_event[PerSecondLatestEventTracker.DATETIME_OBJ]
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
        marshaled_event[PerSecondLatestEventTracker.DATETIME_OBJ] = self.__round_seconds(dt)

        return marshaled_event


class TransactionType(Enum):
    SHORT = 1
    LONG = 2


class ScoreFunctions:
    def __init__(self, volume_median, price_percentage_diff_threshold, security_code,
                 bse_announcement_crawler: BseAnnouncementCrawler):
        self._price_percentage_diff_threshold = price_percentage_diff_threshold
        self._base_filter_volume_threshold = self.get_vol_threshold(volume_median, 6 * 60 * 60)
        self._vol_diff_threshold_at_second_level = self._base_filter_volume_threshold / 12
        self._score_sum_threshold = 4
        self._market_open_time = datetime.now().replace(hour=9, minute=15, second=0)
        self._security_code = security_code
        self._bse_announcement_crawler = bse_announcement_crawler

    def _update_result_time(self):
        self._bse_announcement_crawler.get_company_announcement_map_for_today()

    def long_score_func_list(self):
        return [self.volume_score_function, self.long_price_quantity_score, self.result_score]

    def short_score_func_list(self):
        return [self.volume_score_function, self.short_price_quantity_score, self.result_score]

    def base_filter(self, q: list) -> bool:
        start_end_vol_diff = self.start_end_diff(q, VOLUME)
        oldest_elem = q[0]
        oldest_elem_time = oldest_elem[PerSecondLatestEventTracker.DATETIME_OBJ]
        seconds_till_now = (oldest_elem_time - self._market_open_time).total_seconds()

        if seconds_till_now > 2 * 60 * 60:
            moving_vol_threshold = self.get_vol_threshold(oldest_elem[VOLUME], seconds_till_now)
            if moving_vol_threshold > self._base_filter_volume_threshold:
                self._base_filter_volume_threshold = moving_vol_threshold
                self._vol_diff_threshold_at_second_level = self._base_filter_volume_threshold / 12

        return start_end_vol_diff > self._base_filter_volume_threshold

    @staticmethod
    def get_vol_threshold(vol_till_now, seconds_till_now):
        total_15_sec_slots = seconds_till_now / 15
        per_15_sec_slot_vol = vol_till_now / total_15_sec_slots
        return per_15_sec_slot_vol * 20

    def volume_score_function(self, q: list) -> int:
        score = 0
        for i in range(1, 5):
            score = score + 1 if q[-i][VOLUME] - q[-i - 1][VOLUME] > self._vol_diff_threshold_at_second_level \
                else score
        return score

    def result_score(self, q: list) -> int:
        result_time = self._bse_announcement_crawler.get_latest_result_time_for_security_code(self._security_code)
        q_time = q[-1][PerSecondLatestEventTracker.DATETIME_OBJ]
        td = q_time - result_time
        return (0 <= td.total_seconds() < 10 * 60) * 2

    def long_price_quantity_score(self, q: list) -> int:
        price_diff = self.start_end_diff(q, LAST_PRICE)
        buy_quantity_diff = self.start_end_diff(q, BUY_QUANTITY)
        sell_quantity_diff = self.start_end_diff(q, SELL_QUANTITY)

        score = 0
        if price_diff > 0 and self.price_diff_threshold_breached(price_diff, q):
            score = score + 1 if buy_quantity_diff > 0 else score
            score = score + 1 if sell_quantity_diff < 0 else score
        return score

    def short_price_quantity_score(self, q: list) -> int:
        price_diff = self.start_end_diff(q, LAST_PRICE)
        buy_quantity_diff = self.start_end_diff(q, BUY_QUANTITY)
        sell_quantity_diff = self.start_end_diff(q, SELL_QUANTITY)

        score = 0
        if price_diff < 0 and self.price_diff_threshold_breached(price_diff, q):
            score = score + 1 if buy_quantity_diff < 0 else score
            score = score + 1 if sell_quantity_diff > 0 else score
        return score

    def price_diff_threshold_breached(self, price_diff, q):
        return abs(price_diff) > (self._price_percentage_diff_threshold * q[0][LAST_PRICE]) / 100

    @staticmethod
    def start_end_diff(q, key):
        return q[-1][key] - q[0][key]

    def score_filter(self, score_list: list) -> bool:
        return all([score > 0 for score in score_list]) * sum(score_list) > self._score_sum_threshold


class MarketPosition:
    def __init__(self):
        self._entry_scores = None
        self._entry_event = None
        self._exit_event = None
        self._transaction_type = None
        self._took_position = False
        self._trade_completed = False

    def enter(self, entry_event, transaction_type: TransactionType, scores: list):
        self._entry_event = entry_event
        self._transaction_type = transaction_type
        self._entry_scores = scores
        self._took_position = True
        msg = "buy" if transaction_type == TransactionType.LONG else "sell"
        logger.info("{} stocks at : ".format(msg) + str(entry_event))

    def entry_price(self):
        return self._entry_event[LAST_PRICE]

    def is_profitable_diff(self, diff):
        return (self._transaction_type == TransactionType.LONG and diff > 0) \
               or (self._transaction_type == TransactionType.SHORT and diff < 0)

    def exit(self, exit_event):
        self._exit_event = exit_event
        self._trade_completed = True

    def is_trade_done(self):
        return self._trade_completed

    def get_summary(self):
        return {
            'entry': self._entry_event,
            'exit': self._exit_event,
            'score': self._entry_scores,
            'type': str(self._transaction_type)
        }


class MarketChangeDetector:
    def __init__(self, window_len, score_functions: ScoreFunctions, trading_sym, kite_connect: KiteConnect):
        self._base_filter_func = score_functions.base_filter
        self._long_score_func_list = score_functions.long_score_func_list()
        self._short_score_func_list = score_functions.short_score_func_list()
        self._score_filter_func = score_functions.score_filter

        keys_to_track = [EMPTY_KEY, TIMESTAMP, VOLUME, BUY_QUANTITY, SELL_QUANTITY, LAST_TRADE_TIME,
                         LAST_PRICE]
        self._event_window_15_sec = PerSecondLatestEventTracker(window_length_in_seconds=window_len,
                                                                keys_to_track=keys_to_track)

        self._filter_pass_key_name = 'bool'
        self._filter_pass_queue = PerSecondLatestEventTracker(window_length_in_seconds=window_len,
                                                              keys_to_track=[self._filter_pass_key_name])

        self._profit_limit = 0.01
        self._loss_limit = 0.01

        self._position = MarketPosition()

        self._trade_completed = False
        self._kite_connect = kite_connect
        self._trading_sym = trading_sym

    def run(self, market_event):
        # if not self._position.is_trade_done():
        #     if not self._position._took_position:
        #         self._try_take_position(market_event)
        #     else:
        #         self._try_exiting(market_event)
        if not self._position._took_position:
            self._try_take_position(market_event)

    def _try_exiting(self, market_event):
        entry_price = self._position.entry_price()
        diff = market_event[LAST_PRICE] - entry_price
        abs_change = diff if diff > 0 else diff * -1
        if self._position.is_profitable_diff(diff):
            if abs_change > entry_price * self._profit_limit:
                self._position.exit(market_event)
        else:
            if abs_change > entry_price * self._loss_limit:
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
                self._position.enter(market_event, TransactionType.LONG, long_scores)
                self._execute_kite_trade(market_event, TransactionType.LONG)
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
            elif self._score_filter_func(short_scores):
                self._position.enter(market_event, TransactionType.SHORT, short_scores)
                self._execute_kite_trade(market_event, TransactionType.SHORT)
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
            else:
                set_flag_with_base_filter_func()

        else:
            set_flag_with_base_filter_func()

    def _execute_kite_trade(self, market_event, transaction_type: TransactionType):
        if transaction_type == TransactionType.LONG:
            entry_price = market_event['depth']['sell'][0]['price']
            kite_transaction_type = "BUY"
            square_off = entry_price * 1.05
        else:
            entry_price = market_event['depth']['buy'][0]['price']
            kite_transaction_type = "SELL"
            square_off = entry_price * 0.95
        stop_loss = entry_price * 0.015
        trailing_stop_loss = entry_price * 0.01

        open_price = market_event['ohlc']['open']
        price_diff_percentage = (100 * abs(open_price - entry_price)) / open_price
        if entry_price > 1500 or price_diff_percentage > 5:
            print("not executing trade in kite as entry_price was: {} and price_diff_percentage: {}"
                  .format(entry_price, price_diff_percentage))
        else:
            self._kite_connect.place_order(variety=Variety.BRACKET.value,
                                           exchange=Exchange.NSE.value,
                                           tradingsymbol=self._trading_sym,
                                           transaction_type=kite_transaction_type,
                                           quantity=1,
                                           product=PRODUCT.MIS.value,
                                           order_type=OrderType.LIMIT.value,
                                           validity=VALIDITY.DAY.value,
                                           squareoff=square_off, stoploss=stop_loss,
                                           trailing_stoploss=trailing_stop_loss,
                                           price=entry_price)

    @staticmethod
    def debug_point(current_event_list_view):
        if current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].hour == 14 \
                and current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].minute == 54 \
                and current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].second == 7:
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
    def __init__(self):
        with open('./config.yml') as handle:
            config = yaml.load(handle)
        self._postgres = PostgresIO(config['postgres-config'])
        self._postgres.connect()

        self._bse = BseUtil(config, self._postgres)
        self._bse_announcement_crawler = BseAnnouncementCrawler(self._postgres, config)
        self._k_util = KiteUtil(self._postgres, config)

        self._market_stats = self._bse.get_all_stats()
        self._market_change_detector_dict = {}

        session_info = self._k_util.get_current_session_info()['result'][0]
        self._instruments_to_fetch = self._get_instruments_to_fetch()
        self._kws = KiteTicker(session_info['api_key'], session_info['access_token'])
        self._instruments_to_ignore = set()
        self._kite_connect = KiteConnect(session_info['api_key'], session_info['access_token'])

    def _volume_median_for_instrument_code(self, trading_sym):
        stat = list(filter(lambda j: j['symbol'] == trading_sym, self._market_stats))[0]
        return float(stat['volume_median'])

    def _get_market_change_detector(self, instrument_code) -> MarketChangeDetector:
        def _create_market_change_detector():
            score_func = ScoreFunctions(self._volume_median_for_instrument_code(trading_sym), 0.2, security_code,
                                        self._bse_announcement_crawler)
            return MarketChangeDetector(15, score_func, trading_sym, self._kite_connect)

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

    def run(self):
        def on_ticks(ws, ticks):
            # Callback to receive ticks.
            self.handle_ticks_safely(ticks)
            # for tick in ticks:
            #     self._get_market_change_detector(str(tick['instrument_token'])).run(tick)
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

        self._kws.on_ticks = on_ticks
        self._kws.on_connect = on_connect
        self._kws.on_close = on_close

        self._kws.connect()

    def handle_ticks_safely(self, ticks):
        for index in range(len(ticks)):
            try:
                if ticks[index]['instrument_token'] not in self._instruments_to_ignore:
                    self._get_market_change_detector(str(ticks[index]['instrument_token'])).run(ticks[index])
            except Exception as e:
                traceback.print_exc()
                logger.error("ignoring instrument at instrument_token: {}".format(ticks[index]['instrument_token']))
                self._instruments_to_ignore.add(ticks[index]['instrument_token'])

    def get_summary(self):
        summaries = {}
        for key in self._market_change_detector_dict.keys():
            mcd = self._market_change_detector_dict[key]
            summaries[key] = mcd.get_summary()
        return summaries

    def place_order(self, stock_code):
        place_order(self._kite_connect, stock_code)


if __name__ == '__main__':
    MainClass().run()
