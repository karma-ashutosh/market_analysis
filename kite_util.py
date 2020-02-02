import yaml
from kiteconnect import KiteConnect

from postgres_io import PostgresIO


class KiteUtil:
    def __init__(self, postgres: PostgresIO, config: dict):
        self.__postgres = postgres
        self.__config = config
        self.__instrument_mapping_table = config['kite_config']['instrument_mapping_table']
        self.__bse_nse_mapping_table = config['kite_config']['bse_nse_mapping']
        self.__session_info_table = config['kite_config']['session_info_table']
        self._instrument_id_to_security_code_cache = {}

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

    def map_nse_code_to_instrument_id(self, nse_codes: list) -> dict:
        nse_code_input = "('" + "','".join(nse_codes) + "')"
        query = ["SELECT * FROM {} WHERE exchange='NSE' and segment = 'NSE' and exchange_token in {}"
                     .format(self.__instrument_mapping_table, nse_code_input)]
        result = self.__postgres.execute(query, fetch_result=True)['result']
        mapping = {}
        for entry in result:
            mapping[entry['exchange_token']] = entry['instrument_token']
        return mapping

    def map_bse_code_to_instrument_id(self, bse_codes: list) -> dict:
        bse_code_input = "('" + "','".join(bse_codes) + "')"
        query = ["SELECT * FROM {} WHERE exchange='BSE' and segment = 'BSE' and exchange_token in {}"
                     .format(self.__instrument_mapping_table, bse_code_input)]
        result = self.__postgres.execute(query, fetch_result=True)['result']
        # result.extend(self.get_nse_counterpart_instrument_results(result))
        mapping = {}
        for entry in result:
            mapping[entry['exchange_token']] = entry['instrument_token']
        return mapping

    def map_instrument_ids_to_trading_symbol(self) -> dict:
        query = ["SELECT * FROM {}".format(self.__instrument_mapping_table)]
        result = self.__postgres.execute(query, fetch_result=True)['result']
        mapping = {}
        for entry in result:
            mapping[entry['instrument_token']] = entry['tradingsymbol']
        return mapping

    def map_instrument_ids_to_trading_symbol_security_code(self, instrument_token) -> tuple:
        if instrument_token not in self._instrument_id_to_security_code_cache.keys():
            query = ["SELECT * FROM {} WHERE instrument_token='{}'".format(self.__instrument_mapping_table, instrument_token)]
            result = self.__postgres.execute(query, fetch_result=True)['result']
            entry = result[0]
            self._instrument_id_to_security_code_cache[instrument_token] = (entry['tradingsymbol'], entry['exchange_token'])
        return self._instrument_id_to_security_code_cache[instrument_token]

    def get_nse_exchange_token_for_bse_exchange_token(self, bse_codes) -> dict:
        bse_code_input = "('" + "','".join(bse_codes) + "')"
        query = ["SELECT nse_exchange_token, bse_exchange_token FROM {} WHERE bse_exchange_token in {}"
                     .format(self.__bse_nse_mapping_table, bse_code_input)]
        result = self.__postgres.execute(query, fetch_result=True)['result']
        mapping = {}
        for entry in result:
            mapping[entry['bse_exchange_token']] = entry['nse_exchange_token']
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
