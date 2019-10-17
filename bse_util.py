import json
import requests

from datetime import datetime, timedelta
from postgres_io import PostgresIO
from general_util import csv_file_with_headers_to_json_arr


class BseUtil:
    def __init__(self, config: dict, postgres: PostgresIO):
        self.__config = config
        self.__postgres = postgres
        self.__upcoming_results_date_table = config['bse_send_result_notification']['upcoming_result_table']
        self.__stock_date_wise_monitoring_time_window = self.__prepare_significant_stock_time_ranges()

    def __prepare_significant_stock_time_ranges(self) -> dict:
        historical_result_file = self.__config['bse_send_result_notification']['historical_result_time_info_file']
        j_arr = csv_file_with_headers_to_json_arr(historical_result_file)
        result = {}
        for j_elem in j_arr:
            dt = datetime.strptime(j_elem.get('date_time'), '%Y-%m-%d %H:%M:%S')
            j_elem['end_time'] = dt + timedelta(minutes=30)
            j_elem['start_time'] = dt - timedelta(minutes=30)
            result["{}-{}".format(j_elem.get("stock_symbol"), dt.date())] = j_elem
        return result

    def get_results_announced_for_today(self):
        today = datetime.now()
        today_date = '{}-{}-{}'.format(str(today.year).zfill(4), str(today.month).zfill(2), str(today.day).zfill(2))
        upcoming_results_query = "SELECT * FROM {} WHERE system_readable_date='{}'"\
            .format(self.__upcoming_results_date_table, today_date)
        result_list = self.__postgres.execute([upcoming_results_query], fetch_result=True).get('result', [])
        return result_list

    def get_results_announced_for_yesterday(self):
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_date = '{}-{}-{}'.format(str(yesterday.year).zfill(4), str(yesterday.month).zfill(2), str(yesterday.day).zfill(2))
        upcoming_results_query = "SELECT * FROM {} WHERE system_readable_date='{}'"\
            .format(self.__upcoming_results_date_table, yesterday_date)
        result_list = self.__postgres.execute([upcoming_results_query], fetch_result=True).get('result', [])
        return result_list

    def should_process_historical_event(self, stock_code: str, event_time: datetime) -> bool:
        result = False
        key = "{}-{}".format(stock_code, event_time.date())
        if key in self.__stock_date_wise_monitoring_time_window.keys():
            j_elem = self.__stock_date_wise_monitoring_time_window.get(key)
            if j_elem['start_time'] <= event_time <= j_elem['end_time']:
                result = True
        return result


def get_announcement_for_stock_for_date_range(stock_code, from_date, to_date) -> list:
    announcement_url_format = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strCat=-1&strPrevDate={" \
                              "}&strScrip={}&strSearch=P&strToDate={}&strType=C "
    url = announcement_url_format.format(from_date, stock_code, to_date)
    r = requests.get(url)
    json_res = json.loads(r.text)
    result = []
    if json_res.get('Table'):
        result = json_res.get('Table')
    return result
