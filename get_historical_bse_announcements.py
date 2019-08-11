import json
import yaml

from bse_util import BseUtil, get_announcement_for_stock_for_date_range
from postgres_io import PostgresIO
from result_date_object import ResultDate

with open('./config.yml') as handle:
    config = yaml.load(handle)
postgres = PostgresIO(config['postgres-config'])
postgres.connect()
bse = BseUtil(config, postgres)
upcoming_results_date_table = config['bse_send_result_notification']['upcoming_result_table']
bse_notification_checkpointing_table = config['bse_send_result_notification']['checkpointing_table']


def get_result_announcements_date_range(from_date, to_date):
    query = [
        "SELECT security_code, system_readable_date FROM {} WHERE system_readable_date >= '{}' and "
        "system_readable_date <= '{}'".format(upcoming_results_date_table, from_date, to_date)]
    rows = postgres.execute(query, fetch_result=True)['result']
    announcement_list = []
    for row in rows:
        result_date = ResultDate(row)
        announcement_date = result_date.system_readable_date.replace("-", "")
        announcement = get_announcement_for_stock_for_date_range(result_date.security_code, announcement_date,
                                                                 announcement_date)
        if announcement:
            announcement_list.append(announcement)
    return announcement_list


def run():
    from_date = input("Enter the from date (in format '2019-08-04'):\t")
    to_date = input("Enter the to date (in format '2019-08-04'):\t")
    output_location = input("output location to save your data (example /tmp/result.json): ")

    announcement_list = get_result_announcements_date_range(from_date, to_date)
    output = []
    for announcement in announcement_list:
        output.extend(announcement)
    with open(output_location, 'w') as h:
        json.dump(output, h, indent=1)


if __name__ == '__main__':
    run()
