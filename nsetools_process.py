from datetime import datetime, timedelta
from time import sleep

import yaml
from nsetools import Nse

from general_util import run_in_background, setup_logger
from postgres_io import PostgresIO

logger = setup_logger("nse_logger", "./log_nsetools.log")

with open('./config.yml') as handle:
    config = yaml.load(handle)
postgres = PostgresIO(config['postgres-config'])
postgres.connect()

upcoming_results_date_table = config['nse_tools_config']['nse_tools_upcoming_result_table']
nse_tools_result_table = config['nse_tools_config']['nse_tools_result_table']

nse = Nse()


def is_market_active(d: datetime):
    weekday = d.weekday()
    if weekday > 4:
        return False
    hour = d.hour

    if hour > 17 or hour < 8:
        return False

    return True


def should_process_result(result):
    result_date = datetime.strptime(result['result_date'], '%d %B %Y')
    before_days = result['pre_delta_days']
    after_days = result['post_delta_days']

    start_datetime = result_date - timedelta(days=before_days)
    end_datetime = result_date + timedelta(days=after_days)

    today = datetime.now()

    return is_market_active(today) and start_datetime <= today <= end_datetime


def get_nse_data(code):
    logger.info("getting data for code: "+code)
    return nse.get_quote(code)


def process():
    upcoming_results_query = "SELECT * FROM {}".format(upcoming_results_date_table)
    result_list = postgres.execute([upcoming_results_query], fetch_result=True)['result']
    results_to_be_processed = list(filter(should_process_result, result_list))
    logger.info("results to be processed: {}".format(results_to_be_processed))

    result_arr = []

    keys = ('baseprice', 'stock_code', 'adhocmargin', 'applicablemargin', 'averageprice', 'bcenddate', 'bcstartdate',
            'buyprice1', 'buyprice2', 'buyprice3', 'buyprice4', 'buyprice5', 'buyquantity1', 'buyquantity2',
            'buyquantity3', 'buyquantity4', 'buyquantity5', 'change', 'closeprice', 'cm_adj_high_dt', 'cm_adj_low_dt',
            'cm_ffm', 'companyname', 'css_status_desc', 'dayhigh', 'daylow', 'deliveryquantity',
            'deliverytotradedquantity', 'exdate', 'extremelossmargin', 'facevalue', 'high52', 'indexvar', 'isincode',
            'lastprice', 'low52', 'markettype', 'ndenddate', 'ndstartdate', 'open', 'pchange', 'previousclose',
            'priceband', 'pricebandlower', 'pricebandupper', 'purpose', 'quantitytraded', 'recorddate', 'secdate',
            'securityvar', 'sellprice1', 'sellprice2', 'sellprice3', 'sellprice4', 'sellprice5', 'sellquantity1',
            'sellquantity2', 'sellquantity3', 'sellquantity4', 'sellquantity5', 'series', 'symbol', 'totalbuyquantity',
            'totalsellquantity', 'totaltradedvalue', 'totaltradedvolume', 'varmargin', 'query_time')

    for result in results_to_be_processed:
        try:
            logger.info("result is " + str(result))
            stock_quote = get_nse_data(result['security_name'])
            nse_data = {'stock_code': result['security_name'], 'query_time': datetime.now()}
            for key in keys:
                nse_data[key.lower()] = str(stock_quote.get(key))
            result_arr.append(nse_data)
        except Exception as e:
            logger.error(e)

    postgres.insert_jarr(result_arr, nse_tools_result_table)


if __name__ == '__main__':
    while True:
        run_in_background(process)
        sleep(10)
