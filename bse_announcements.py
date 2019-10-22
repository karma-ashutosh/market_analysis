import json
from datetime import datetime
from time import sleep

import requests
import yaml

from general_util import run_in_background, setup_logger, send_mail
from postgres_io import PostgresIO
from bse_util import BseUtil

logger = setup_logger("bse_logger", "./logs/log_bse_announcements.log")
time_logger = setup_logger("time_logger", "./logs/time_logger.log")

with open('./config.yml') as handle:
    config = yaml.load(handle)
postgres = PostgresIO(config['postgres-config'])
postgres.connect()


bse_notification_checkpointing_table = config['bse_send_result_notification']['checkpointing_table']
mail_username = config['email-config']['username']
mail_password = config['email-config']['password']


bse = BseUtil(config, postgres)


def get_already_processed_news_ids(stock_code) -> list:
    start_time = datetime.now()
    processed_news_query = "SELECT * FROM {} WHERE stock_code='{}'".format(bse_notification_checkpointing_table,
                                                                           stock_code)
    rows = postgres.execute([processed_news_query], fetch_result=True)['result']
    processed_news_ids = []
    if len(rows) > 0:
        for row in rows:
            if row['send_mail_alert_processing']:
                processed_news_ids.append(row['news_id'])
    end_time = datetime.now()
    time_logger.info("Function: {}, start_time: {}, end_time: {}".format("get_already_processed_news_ids",
                                                                         str(start_time), str(end_time)))
    return processed_news_ids


def mark_announcement_as_processed(stock_code: str, announcements: list):
    start_time = datetime.now()
    payloads = [{'news_id': announcement['NEWSID'], 'send_mail_alert_processing': True, 'stock_code': stock_code}
                for announcement in announcements]
    postgres.insert_or_skip_on_conflict(payloads, bse_notification_checkpointing_table, ['stock_code', 'news_id'])
    end_time = datetime.now()
    time_logger.info("Function: {}, start_time: {}, end_time: {}".format("mark_announcement_as_processed",
                                                                         str(start_time), str(end_time)))


def get_todays_annoucement_for_stock(stock_code) -> list:
    start_time = datetime.now()
    annoucement_url_format = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strCat=-1&strPrevDate={" \
                             "}&strScrip={}&strSearch=P&strToDate={}&strType=C "
    # date format -> 20190710 == 10th July, 2019
    url_date= "{}{}{}".format(str(today.year).zfill(4), str(today.month).zfill(2), str(today.day).zfill(2))
    url = annoucement_url_format.format(url_date, stock_code, url_date)
    r = requests.get(url)
    json_res = json.loads(r.text)
    d = {}
    result = []
    if json_res.get('Table'):
        result = json_res.get('Table')
    end_time = datetime.now()
    time_logger.info("Function: {}, start_time: {}, end_time: {}".format("get_todays_annoucement_for_stock",
                                                                         str(start_time), str(end_time)))
    return result


def process_new_bse_updates_for_stocks_having_result_for_today():
    start_time = datetime.now()
    results_to_be_processed = bse.get_results_announced_for_today()
    logger.info("results to be processed: {}".format(results_to_be_processed))
    processing_response = []
    for result in results_to_be_processed:
        try:
            processing_response.append(send_notification_if_new_bse_update_available(result))
        except Exception as e:
            logger.error(e)
    logger.info("Processing successful. Processed metadata is as:\t{}".format(json.dumps(processing_response)))
    end_time = datetime.now()
    time_logger.info("Function: {}, start_time: {}, end_time: {}".format("process_new_bse_updates_for_stocks_having_result_for_today",
                                                                         str(start_time), str(end_time)))


def send_notification_if_new_bse_update_available(stock_metadata) -> dict:
    start_time = datetime.now()
    logger.info("stock_metadata being processed is " + str(stock_metadata))
    stock_code = stock_metadata['security_code']
    unprocessed_announcements = fetch_new_announcements_from_bse(stock_code)
    if len(unprocessed_announcements) > 0:
        security_name = stock_metadata['security_name']
        send_notification_for_announcements(security_name, unprocessed_announcements)
        mark_announcement_as_processed(stock_code, unprocessed_announcements)
    result = {
        'stock_meta': stock_metadata,
        'processed_announcement_count': len(unprocessed_announcements),
        'news_ids': [announcement['NEWSID'] for announcement in unprocessed_announcements]
    }
    end_time = datetime.now()
    time_logger.info("Function: {}, start_time: {}, end_time: {}".format("send_notification_if_new_bse_update_available",
                                                                         str(start_time), str(end_time)))

    return result


def fetch_new_announcements_from_bse(stock_code):
    start_time = datetime.now()
    processed_news_ids = get_already_processed_news_ids(stock_code)
    announcements = get_todays_annoucement_for_stock(stock_code)
    unprocessed_announcements = list(filter(lambda announcement:
                                            announcement['NEWSID'] not in processed_news_ids, announcements))
    end_time = datetime.now()
    time_logger.info("Function: {}, start_time: {}, end_time: {}".format("fetch_new_announcements_from_bse",
                                                                         str(start_time), str(end_time)))

    return unprocessed_announcements


def send_notification_for_announcements(security_name: str, unprocessed_announcements: list):
    start_time = datetime.now()
    file_path = "bse_notifications/{}-{}-{}-{}T{}:{}:{}.json".format(security_name,
                                                                today.year, today.month, today.day,
                                                                today.hour, today.minute, today.second)
    with open(file_path, 'w') as handle:
        json.dump(unprocessed_announcements, handle, indent=1)
    send_mail(mail_username, mail_password, "Notification for {}".format(security_name),
              "PFA",
              ["sethitanmay.work@gmail.com", "prateektagde@gmail.com", "karmav44990@gmail.com"], [file_path])
    end_time = datetime.now()
    time_logger.info("Function: {}, start_time: {}, end_time: {}".format("send_notification_for_announcements",
                                                                         str(start_time), str(end_time)))


if __name__ == '__main__':
    """
    
board meeting


strCat: Board Meeting
strPrevDate: 20191011
strScrip: 
strSearch: P
strToDate: 20191011
strType: C


Result:

strCat: Result
strPrevDate: 20191011
strScrip: 
strSearch: P
strToDate: 20191011
strType: C



All
strCat: -1
strPrevDate: 20191011
strScrip: 
strSearch: P
strToDate: 20191011
strType: C
    """
    """https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w?strCat=-1&strPrevDate=20191011&strScrip=&strSearch=P&strToDate=20191011&strType=C"""
    process_new_bse_updates_for_stocks_having_result_for_today()
    while True:
        try:
            today = datetime.now()
            run_in_background(process_new_bse_updates_for_stocks_having_result_for_today)
            sleep(10)
        except:
            logger.exception("Exception while executing process_new_bse_updates_for_stocks_having_result_for_today: gi")
