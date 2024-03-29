from kiteconnect import KiteConnect, KiteTicker
from bse_util import BseUtil
from kite_util import KiteUtil
from postgres_io import PostgresIO


class ConnectionFactory:
    def __init__(self, config):
        self.config = config
        self.postgres = None
        self.kite_headers = None
        self.bse_util = None
        self.kite_connect = None
        self.kite_web_streaming_ticker = None

    def init_all(self):
        self.verify_or_init_posgres()
        self.init_kite_for_api()
        self.init_bse_util()

    def verify_or_init_posgres(self):
        if not self.postgres:
            self.postgres = PostgresIO(self.config['postgres-config'])
            # commented below as not running postgres as of now. To be removed once we start using db
            # postgres.connect()
        else:
            print("Not initializing postgres as already initialized")

    def init_kite_for_api(self):
        self.verify_or_init_posgres()
        k_util = KiteUtil(self.postgres, self.config)
        session_info = k_util.get_current_session_info()['result'][0]
        api_key, access_token = session_info['api_key'], session_info['access_token']

        self.kite_connect = KiteConnect(api_key=api_key, access_token=access_token)
        self.kite_connect.login_url()
        self.kite_web_streaming_ticker = KiteTicker(api_key, access_token)
        self.kite_headers = {'X-Kite-Version': '3', 'Authorization': 'token {}:{}'.format(api_key, access_token)}
        return self

    def init_bse_util(self):
        self.verify_or_init_posgres()
        self.bse_util = BseUtil(self.config, self.postgres)
