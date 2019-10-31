import json
from datetime import datetime
from datetime import timedelta
from enum import Enum
from queue import Queue

import yaml
from kiteconnect import KiteTicker

from bse_util import BseUtil, BseAnnouncementCrawler
from general_util import csv_file_with_headers_to_json_arr, json_arr_to_csv, flatten
from general_util import setup_logger
from kite_util import KiteUtil
from postgres_io import PostgresIO
from stock_data_analysis import PerSecondLatestEventTracker, TransactionType


stock_logger = setup_logger("stock_logger", "/data/kite_websocket_data/stock.log", msg_only=True)
msg_logger = setup_logger("msg_logger", "/tmp/app.log")

EMPTY_KEY = ''

LAST_PRICE = 'last_price'

LAST_TRADE_TIME = 'last_trade_time'

SELL_QUANTITY = 'sell_quantity'

BUY_QUANTITY = 'buy_quantity'

VOLUME = 'volume'


class ScoreFunctions:
    def __init__(self, median, price_percentage_diff_threshold, result_time: datetime):
        self._price_percentage_diff_threshold = price_percentage_diff_threshold
        self._result_time = result_time + timedelta(
            seconds=10)  # for changing the time to react from result announcement
        self._base_filter_volume_threshold = self.get_vol_threshold(median, 6 * 60 * 60)
        self._vol_diff_threshold_at_second_level = self._base_filter_volume_threshold / 12
        self._score_sum_threshold = 4
        self._market_open_time = self._result_time.replace(hour=9, minute=15, second=0)

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
        q_time = q[-1][PerSecondLatestEventTracker.DATETIME_OBJ]
        td = q_time - self._result_time
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
        return all([score > 0 for score in score_list[:-1]]) * sum(score_list) > self._score_sum_threshold


class MarketChangeDetector:
    """
                return MarketChangeDetector(self._string_date_key, 15, self.base_filter,
                                        [self.volume_score_function, self.long_price_quantity_score, self.result_score],
                                        [self.volume_score_function, self.short_price_quantity_score, self.result_score],
                                        self.score_filter)

    """
    def __init__(self, string_date_key, window_len, score_functions: ScoreFunctions):
        self._base_filter_func = score_functions.base_filter
        self._long_score_func_list = score_functions.long_score_func_list()
        self._short_score_func_list = score_functions.short_score_func_list()
        self._score_filter_func = score_functions.score_filter

        self._string_date_key = string_date_key

        keys_to_track = [EMPTY_KEY, self._string_date_key, VOLUME, BUY_QUANTITY, SELL_QUANTITY, LAST_TRADE_TIME,
                         LAST_PRICE]
        self._event_window_15_sec = PerSecondLatestEventTracker(window_length_in_seconds=window_len,
                                                                keys_to_track=keys_to_track,
                                                                string_date_key=self._string_date_key)

        self._filter_pass_key_name = 'bool'
        self._filter_pass_queue = PerSecondLatestEventTracker(window_length_in_seconds=window_len,
                                                              keys_to_track=[self._filter_pass_key_name],
                                                              string_date_key=self._string_date_key)

        self._trade_completed = False

        self._profit_limit = 0.01
        self._loss_limit = 0.01

        self._took_position = False
        self._entry_score = None
        self._entry_event = None
        self._exit_event = None
        self._transaction_type = None

    def run(self, market_event):
        if not self._trade_completed:
            if not self._took_position:
                self._try_take_position(market_event)
            else:
                if self._try_exiting(market_event):
                    self._trade_completed = True

    def get_summary(self):
        return {
            'entry': self._entry_event,
            'exit': self._exit_event,
            'score': self._entry_score,
            'type': str(self._transaction_type)
        }

    def _entry_function(self, market_event, transaction_type: TransactionType, scores: list):
        msg = "buy" if transaction_type == TransactionType.LONG else "sell"
        self._entry_event = market_event
        self._entry_score = scores
        self._transaction_type = transaction_type
        print("{} stocks at : ".format(msg) + str(q[-1]))

    def _is_profit(self, diff):
        return (self._transaction_type == TransactionType.LONG and diff > 0) \
               or (self._transaction_type == TransactionType.SHORT and diff < 0)

    def _try_exiting(self, market_event) -> bool:
        diff = self._entry_event[LAST_PRICE] - market_event[LAST_PRICE]
        abs_change = abs(diff)
        if self._is_profit(diff):
            if abs_change > self._entry_event[LAST_PRICE] * self._profit_limit:
                self._exit_event = market_event
                return True
            else:
                return False
        else:
            if abs_change > self._entry_event[LAST_PRICE] * self._loss_limit:
                self._exit_event = market_event
                return True
            else:
                return False

    def _try_take_position(self, market_event):
        def set_flag_with_base_filter_func():
            if self._base_filter_func(current_event_list_view):
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
            else:
                self._filter_pass_queue.move(self.get_filter_event(market_event, False))

        self._event_window_15_sec.move(market_event)
        current_event_list_view = self._event_window_15_sec.get_current_queue_snapshot()
        if any([e[self._filter_pass_key_name] for e in self._filter_pass_queue.get_current_queue_snapshot()]):
            if current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].hour == 14 \
                    and current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].minute == 59 \
                    and current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].second > 10:
                x = 0
            long_scores = list(map(lambda func: func(current_event_list_view), self._long_score_func_list))
            short_scores = list(map(lambda func: func(current_event_list_view), self._short_score_func_list))
            if self._score_filter_func(long_scores):
                self._entry_function(market_event, TransactionType.LONG, long_scores)
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
                self._took_position = True
            elif self._score_filter_func(short_scores):
                self._entry_function(market_event, TransactionType.SHORT, short_scores)
                self._filter_pass_queue.move(self.get_filter_event(market_event, True))
                self._took_position = True
            else:
                set_flag_with_base_filter_func()

        else:
            set_flag_with_base_filter_func()

    @staticmethod
    def debug_point(current_event_list_view):
        if current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].hour == 14 \
                and current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].minute == 54 \
                and current_event_list_view[-1][PerSecondLatestEventTracker.DATETIME_OBJ].second == 7:
            x = 0

    def get_filter_event(self, market_event, state: bool):
        filter_event = {
            self._string_date_key: market_event.get(self._string_date_key),
            self._filter_pass_key_name: state
        }
        return filter_event


class MainClass:
    def __init__(self, file_name, median, price_percentage_diff_threshold, result_time: datetime, string_date_key):
        self._file_name = file_name
        self._string_date_key = string_date_key
        self._score_functions = ScoreFunctions(median, price_percentage_diff_threshold, result_time)
        self._market_change_detector_dict = {}

    def _get_market_change_detector(self, instrument_code) -> MarketChangeDetector:
        def _create_market_change_detector():
            return MarketChangeDetector(self._string_date_key, 15, self._score_functions)

        if instrument_code not in self._market_change_detector_dict.keys():
            self._market_change_detector_dict[instrument_code] = _create_market_change_detector()

        return self._market_change_detector_dict[instrument_code]

    def run(self):
        def get_instruments_to_fetch():
            results_for_today = bse.get_result_announcement_meta_for_today()
            results_for_yesterday = bse.get_result_announcement_meta_for_yesterday()
            results_for_today.extend(results_for_yesterday)

            security_codes = list(map(lambda j: j['security_code'], results_for_today))
            instrument_mapping = k_util.map_bse_code_to_instrument_id(security_codes)
            return [int(v) for v in instrument_mapping.values()]

        with open('./config.yml') as handle:
            config = yaml.load(handle)
        postgres = PostgresIO(config['postgres-config'])
        postgres.connect()

        bse = BseUtil(config, postgres)
        k_util = KiteUtil(postgres, config)

        session_info = k_util.get_current_session_info()['result'][0]
        instruments = get_instruments_to_fetch()

        kws = KiteTicker(session_info['api_key'], session_info['access_token'])

        def on_ticks(ws, ticks):
            # Callback to receive ticks.
            for tick in ticks:
                self._get_market_change_detector(tick['instrument_code']).run(tick)
                for key in tick.keys():
                    if isinstance(tick[key], datetime):
                        tick[key] = str(tick[key])

            stock_logger.info("{}".format(json.dumps(ticks)))

        def on_connect(ws, response):
            # Callback on successful connect.
            # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).

            ws.subscribe(instruments)

            # Set RELIANCE to tick in `full` mode.
            ws.set_mode(ws.MODE_FULL, instruments)

        def on_close(ws, code, reason):
            # On connection close stop the main loop
            # Reconnection will not happen after executing `ws.stop()`
            ws.stop()

        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.on_close = on_close

        kws.connect()


if __name__ == '__main__':

    stat_file = "../market_analysis_data/stock_stats/combined_stats.json"
    with open(stat_file) as handle:
        stats = json.load(handle)

    summary_arr = csv_file_with_headers_to_json_arr("../market_analysis_data/summary.csv")
    names = list(map(lambda j_elem: j_elem['file_name'], summary_arr))


    def get_median(symbol):
        return list(filter(lambda j_elem: j_elem['stock_identifier'] == symbol, stats))[0]['volume_median']


    def get_result_time(symbol):
        time_elem = list(filter(lambda j_elem: j_elem['file_name'] == symbol, summary_arr))
        if len(time_elem) != 1:
            raise Exception("Bruh.. the result time is fucked up man.. just look at it: " + str(time_elem))
        return datetime.strptime("{} {}".format(time_elem[0]['date'], time_elem[0]['time_value']), '%Y-%m-%d %H:%M:%S')


    results = []


    def func(file_name, string_date_key):
        result = MainClass(file_name + ".csv", get_median(file_name), 0.2, get_result_time(file_name), string_date_key) \
            .run()
        result['file_name'] = file_name
        results.append(result)


    for name in names:
        try:
            try:
                func(name, "timestamp")
            except:
                func(name, "0.timestamp")
        except Exception as e:
            print("failed to process file: " + name)
            # raise e

    json_file_path = "../market_analysis_data/simulation_result.json"
    csv_file_path = "../market_analysis_data/simulation_result.csv"
    with open(json_file_path, 'w') as handle:
        json.dump(results, handle, indent=2)

    flat_j_arr = [flatten(j_elem) for j_elem in results]
    json_arr_to_csv(flat_j_arr, csv_file_path)
