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
        result_list = self.__postgres.execute([upcoming_results_query], fetch_result=True)['result']
        return result_list
