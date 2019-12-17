from abc import abstractmethod
from datetime import datetime

from bse_util import BseAnnouncementCrawler
from general_util import csv_file_with_headers_to_json_arr


class ResultTimeProvider:
    @abstractmethod
    def get_latest_result_time(self, stock_identifier):
        pass


class BseCrawlerBasedResultTimeProvider(ResultTimeProvider):
    def __init__(self, crawler: BseAnnouncementCrawler):
        self._bse_crawler = crawler

    def get_latest_result_time(self, stock_identifier):
        self._bse_crawler.get_latest_result_time_for_security_code(stock_identifier)


class SummaryFileBasedResultTimeProvider(ResultTimeProvider):
    def __init__(self, summary_file_path):
        self._summary_arr = csv_file_with_headers_to_json_arr(summary_file_path)

    def get_latest_result_time(self, stock_identifier):
        time_elem = list(filter(lambda j_elem: j_elem['file_name'] == stock_identifier, self._summary_arr))
        if len(time_elem) != 1:
            raise Exception("Bruh.. the result time is fucked up man.. just look at it: " + str(time_elem))
        return datetime.strptime("{} {}".format(time_elem[0]['date'], time_elem[0]['time_value']), '%Y-%m-%d %H:%M:%S')


