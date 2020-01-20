from datetime import datetime
from time import sleep

import yaml

from bse_util import BseAnnouncementCrawler
from general_util import run_in_background, setup_logger
from postgres_io import PostgresIO

logger = setup_logger("bse_logger", "./logs/log_bse_announcements.log")
time_logger = setup_logger("time_logger", "./logs/time_logger.log")


if __name__ == '__main__':
    # process_new_bse_updates_for_stocks_having_result_for_today()
    with open('./config.yml') as handle:
        config = yaml.load(handle)
    postgres = PostgresIO(config['postgres-config'])
    postgres.connect()

    crawler = BseAnnouncementCrawler(postgres, config)
    while True:
        try:
            today = datetime.now()
            run_in_background(crawler.refresh)
            sleep(2)
        except:
            logger.exception("Exception while executing process_new_bse_updates_for_stocks_having_result_for_today: gi")
