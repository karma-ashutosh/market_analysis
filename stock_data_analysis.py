from queue import Queue

from general_util import csv_file_with_headers_to_json_arr


class MarketEventEmitter:
    def __init__(self, file_name='aparinds_sheet.csv'):
        base_path = '/Users/ashutosh.v/Development/market_analysis_data/'

        j_arr = csv_file_with_headers_to_json_arr(base_path + file_name)
        self.__event_list = list(
            map(lambda j_elem: self.__remove_keys(j_elem, ['depth', 'Unnamed', 'instrument_token', 'mode', 'ohlc', 'oi_day',
                                                    'tradable']),
                j_arr))
        self.__event_iter = iter(self.__event_list)

    @staticmethod
    def __remove_keys(j_elem: dict, text_list_to_remove):
        keys_without_depth = list(
            filter(lambda key: all([text not in key for text in text_list_to_remove]), j_elem.keys()))
        return dict(map(lambda key: (key, j_elem.get(key)), keys_without_depth))

    def emit(self):
        return next(self.__event_iter)


class MarketChangeDetector:
    def __init__(self, event_emitter: MarketEventEmitter):
        self._event_emitter = event_emitter
        keys_to_track = ['buy_quantity']
        self._event_window_15_sec = EventWindow(window_length_seconds=15, keys_to_track=keys_to_track)
        self._event_window_1_min = EventWindow(window_length_seconds=60, keys_to_track=keys_to_track)
        self._event_window_10_min = EventWindow(window_length_seconds=600, keys_to_track=keys_to_track)
        self._event_window_60_min = EventWindow(window_length_seconds=3600, keys_to_track=keys_to_track)

    def run(self):
        while True:
            try:
                market_event = self._event_emitter.emit()
                self._event_window_15_sec.move(market_event)
                self._event_window_1_min.move(market_event)
                self._event_window_10_min.move(market_event)
                self._event_window_60_min.move(market_event)
                avg_15_sec = self._event_window_15_sec.get_avg(key='buy_quantity')
                avg_10_min = self._event_window_10_min.get_avg(key='buy_quantity')

                if avg_15_sec > avg_10_min:
                    print("There is a rise in the share at market_event: {}".format(market_event))
                    print("15 sec avg: {} and 10 min avg: {}".format(avg_15_sec, avg_10_min))
            except StopIteration as e:
                print(e)
                print("Out of events to emit. Market closes. Go smoke all that money")
                break


class EventWindow:
    def __init__(self, window_length_seconds: int, keys_to_track: list):
        self.__window_length = window_length_seconds
        self.__keys_to_track = keys_to_track

        self._events = Queue(maxsize=window_length_seconds)
        self.__initialize_events_queue()

        self._sum = [0] * len(keys_to_track)

    def __initialize_events_queue(self):
        d = {}
        for key in self.__keys_to_track:
            d[key] = 0
        for _ in range(self.__window_length):
            self._events.put_nowait(d)

    def move(self, json_event: dict):
        out_of_window_event = self._events.get_nowait()
        for i in range(len(self.__keys_to_track)):
            key = self.__keys_to_track[i]
            self._sum[i] = self._sum[i] - float(out_of_window_event.get(key, 0)) + float(json_event.get(key, 0))
        self._events.put_nowait(json_event)

    def get_avg(self, key=None):
        if key:
            key_pos = self.__keys_to_track.index(key)
            return self._sum[key_pos] / self.__window_length
        else:
            return [elem/self.__window_length for elem in self._sum]


if __name__ == '__main__':
    event_emitter = MarketEventEmitter()
    change_detector = MarketChangeDetector(event_emitter)
    change_detector.run()
