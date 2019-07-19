import yaml
from kiteconnect import KiteConnect

from postgres_io import PostgresIO


class KiteUtil:
    def __init__(self, postgres: PostgresIO, config: dict):
        self.__postgres = postgres
        self.__config = config
        self.__instrument_mapping_table = config['kite_config']['instrument_mapping_table']
        self.__session_info_table = config['kite_config']['session_info_table']

    def update_token(self):
        api_key = "h0e1gcxywukd7pzf"
        api_secret = "4f9jwaglfa2bunfyuc4tlq2raby3wewd"
        kite = KiteConnect(api_key=api_key)
        req_token = input("login here and get request token: {}\t".format(kite.login_url()))
        session = kite.generate_session(req_token, api_secret=api_secret)
        access_token = session['access_token']
        print("here is your access token: {}".format(access_token))
        self.__postgres.execute(["UPDATE {} SET access_token = '{}' WHERE api_key = '{}'"
                                .format(self.__session_info_table, access_token, api_key)])

    def map_bse_code_to_instrument_id(self, bse_codes: list) -> dict:
        bse_code_input = "('" + "','".join(bse_codes) + "')"
        query = ["SELECT * FROM {} WHERE exchange='BSE' and segment = 'BSE' and exchange_token in {}"
                     .format(self.__instrument_mapping_table, bse_code_input)]
        result = self.__postgres.execute(query, fetch_result=True)['result']
        mapping = {}
        for entry in result:
            mapping[entry['exchange_token']] = entry['instrument_token']
        return mapping

    def get_current_session_info(self):
        query = ['SELECT * FROM {}'.format(self.__session_info_table)]
        return self.__postgres.execute(query, fetch_result=True)


if __name__ == '__main__':
    with open('./config.yml') as handle:
        config = yaml.load(handle)
    postgres = PostgresIO(config['postgres-config'])
    postgres.connect()
    k = KiteUtil(postgres, config)
    k.update_token()

