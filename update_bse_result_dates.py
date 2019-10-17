# security_code, security_name, result_date, pre_delta_days, post_delta_days, system_readable_date
from datetime import datetime

import yaml

from postgres_io import PostgresIO


def get_human_readable_date(date: str) -> str:
    return datetime.strptime(date, '%d %B %Y').strftime('%Y-%m-%d')


def format_date_for_code(nse_date_string: str) -> str:
    return nse_date_string.replace('Apr', 'April').replace('Jun', 'June').replace('Jul', 'July')\
        .replace('Aug', 'August').replace('Oct', 'October')


def get_insert_json(line: str) -> dict:
    print("parsing")
    arr = line.split("\t")
    security_code, security_name, result_date = arr[0], arr[1], format_date_for_code(arr[2])
    return {
        'security_code': security_code,
        'security_name': security_name,
        'result_date': result_date,
        'pre_delta_days': 1,
        'post_delta_days': 1,
        'system_readable_date': get_human_readable_date(result_date)
    }


def extract_jarr_from_file(file_path: str):
    lines = map(lambda line: line.strip(), open(file_path).readlines())
    j_arr = list(map(get_insert_json, lines))
    return j_arr


if __name__ == '__main__':
    with open('./config.yml') as handle:
        config = yaml.load(handle)
    bse_config = config['bse_config']
    postgres = PostgresIO(config['postgres-config'])
    postgres.connect()
    j_arr = extract_jarr_from_file('text_files/result_dates.txt')

    filtered_stock_list = set(map(lambda j_elem: j_elem['security_code'], postgres.execute(
        ["SELECT security_code from {}".format(bse_config['filtered_stock_list_table'])],
        fetch_result=True)['result']))

    target_j_arr = list(filter(lambda j_elem: j_elem['security_code'] in filtered_stock_list, j_arr))
    print("{} stocks qualified for result tracking out of {} stocks provided".format(len(target_j_arr), len(j_arr)))
    postgres.insert_or_skip_on_conflict(target_j_arr, bse_config['upcoming_result_table'],
                                        ['security_code', 'result_date'])