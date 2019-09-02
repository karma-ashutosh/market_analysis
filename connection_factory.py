import yaml

from bse_util import BseUtil
from kite_util import KiteUtil
from postgres_io import PostgresIO

initialized = False
bse_util: BseUtil = None
postgres: PostgresIO = None
kite_util: KiteUtil = None


def initialize():
    global postgres, bse_util, kite_util, initialized
    with open('./config.yml') as handle:
        config = yaml.load(handle)
    postgres = PostgresIO(config['postgres-config'])
    postgres.connect()
    bse_util = BseUtil(config, postgres)
    kite_util = KiteUtil(postgres, config)
    initialized = True


def get_bse_util() -> BseUtil:
    if not initialized:
        initialize()
    return bse_util


def get_postgres_util() -> PostgresIO:
    if not initialized:
        initialize()
    return postgres


def get_kite_util() -> KiteUtil:
    if not initialized:
        initialize()
    return kite_util
