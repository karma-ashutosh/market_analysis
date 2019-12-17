from abc import abstractmethod
from datetime import datetime

from constants import VOLUME, LAST_PRICE, BUY_QUANTITY, SELL_QUANTITY, KITE_EVENT_DATETIME_OBJ
from result_time_provider import ResultTimeProvider


class ScoreFunctions:
    @abstractmethod
    def long_score_func_list(self) -> list:
        pass

    @abstractmethod
    def short_score_func_list(self) -> list:
        pass

    @abstractmethod
    def base_filter(self, event_list):
        pass


class BaseScoreFunctions(ScoreFunctions):
    def __init__(self, volume_median, price_percentage_diff_threshold, security_code,
                 result_time_provider: ResultTimeProvider):
        self._price_percentage_diff_threshold = price_percentage_diff_threshold
        self._base_filter_volume_threshold = self._get_vol_threshold(volume_median, 6 * 60 * 60)
        self._vol_diff_threshold_at_second_level = self._base_filter_volume_threshold / 12
        self._score_sum_threshold = 4
        self._market_open_time = datetime.now().replace(hour=9, minute=15, second=0)
        self._security_code = security_code
        self._result_time_provider = result_time_provider

    def long_score_func_list(self):
        return [self._volume_score_function, self._long_price_quantity_score, self._result_score]

    def short_score_func_list(self):
        return [self._volume_score_function, self._short_price_quantity_score, self._result_score]

    def base_filter(self, q: list) -> bool:
        start_end_vol_diff = self._start_end_diff(q, VOLUME)
        oldest_elem = q[0]
        oldest_elem_time = oldest_elem[KITE_EVENT_DATETIME_OBJ]
        seconds_till_now = (oldest_elem_time - self._market_open_time).total_seconds()

        if seconds_till_now > 2 * 60 * 60:
            moving_vol_threshold = self._get_vol_threshold(oldest_elem[VOLUME], seconds_till_now)
            if moving_vol_threshold > self._base_filter_volume_threshold:
                self._base_filter_volume_threshold = moving_vol_threshold
                self._vol_diff_threshold_at_second_level = self._base_filter_volume_threshold / 12

        return start_end_vol_diff > self._base_filter_volume_threshold

    @staticmethod
    def _get_vol_threshold(vol_till_now, seconds_till_now):
        total_15_sec_slots = seconds_till_now / 15
        per_15_sec_slot_vol = vol_till_now / total_15_sec_slots
        return per_15_sec_slot_vol * 20

    def _volume_score_function(self, q: list) -> int:
        score = 0
        for i in range(1, 5):
            score = score + 1 if q[-i][VOLUME] - q[-i - 1][VOLUME] > self._vol_diff_threshold_at_second_level \
                else score
        return score

    def _result_score(self, q: list) -> int:
        result_time = self._result_time_provider.get_latest_result_time(self._security_code)
        q_time = q[-1][KITE_EVENT_DATETIME_OBJ]
        td = q_time - result_time
        return (0 <= td.total_seconds() < 10 * 60) * 2

    def _long_price_quantity_score(self, q: list) -> int:
        price_diff = self._start_end_diff(q, LAST_PRICE)
        buy_quantity_diff = self._start_end_diff(q, BUY_QUANTITY)
        sell_quantity_diff = self._start_end_diff(q, SELL_QUANTITY)

        score = 0
        if price_diff > 0 and self._price_diff_threshold_breached(price_diff, q):
            score = score + 1 if buy_quantity_diff > 0 else score
            score = score + 1 if sell_quantity_diff < 0 else score
        return score

    def _short_price_quantity_score(self, q: list) -> int:
        price_diff = self._start_end_diff(q, LAST_PRICE)
        buy_quantity_diff = self._start_end_diff(q, BUY_QUANTITY)
        sell_quantity_diff = self._start_end_diff(q, SELL_QUANTITY)

        score = 0
        if price_diff < 0 and self._price_diff_threshold_breached(price_diff, q):
            score = score + 1 if buy_quantity_diff < 0 else score
            score = score + 1 if sell_quantity_diff > 0 else score
        return score

    def _price_diff_threshold_breached(self, price_diff, q):
        return abs(price_diff) > (self._price_percentage_diff_threshold * q[0][LAST_PRICE]) / 100

    @staticmethod
    def _start_end_diff(q, key):
        return q[-1][key] - q[0][key]

