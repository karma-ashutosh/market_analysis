import json
import requests

from datetime import datetime
from postgres_io import PostgresIO


class BseUtil:
    def __init__(self, config: dict, postgres: PostgresIO):
        self.__config = config
        self.__postgres = postgres
        self.__upcoming_results_date_table = config['bse_send_result_notification']['upcoming_result_table']

    def get_results_announced_for_today(self):
        today = datetime.now()
        today_date = '{}-{}-{}'.format(str(today.year).zfill(4), str(today.month).zfill(2), str(today.day).zfill(2))
        upcoming_results_query = "SELECT * FROM {} WHERE system_readable_date='{}'"\
            .format(self.__upcoming_results_date_table, today_date)
        result_list = self.__postgres.execute([upcoming_results_query], fetch_result=True).get('result', [])
        return result_list


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
