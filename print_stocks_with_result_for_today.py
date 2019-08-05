import json
from datetime import datetime
import yaml
from kite_util import KiteUtil
from bse_util import BseUtil
from general_util import setup_logger
from postgres_io import PostgresIO

def get_instruments_to_fetch():
    results_for_today = bse.get_results_announced_for_today()
    return results_for_today 

if __name__ == '__main__':
    with open('./config.yml') as handle:
        config = yaml.load(handle)
    postgres = PostgresIO(config['postgres-config'])
    postgres.connect()

    bse = BseUtil(config, postgres)
    k_util = KiteUtil(postgres, config)

    instruments = get_instruments_to_fetch()
    print(json.dumps(instruments, indent=2))
    

