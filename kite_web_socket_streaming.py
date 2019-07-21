import json

import yaml
from kiteconnect import KiteTicker
from kite_util import KiteUtil
from bse_util import BseUtil
from general_util import setup_logger
from postgres_io import PostgresIO

stock_logger = setup_logger("stock_logger", "./kite_websocket_data/stock.log", msg_only=True)
msg_logger = setup_logger("msg_logger", "/tmp/app.log")


def get_instruments_to_fetch():
    results_for_today = bse.get_results_announced_for_today()
    security_codes = list(map(lambda j: j['security_code'], results_for_today))
    instrument_mapping = k_util.map_bse_code_to_instrument_id(security_codes)
    return [int(v) for v in instrument_mapping.values()]


if __name__ == '__main__':
    with open('./config.yml') as handle:
        config = yaml.load(handle)
    postgres = PostgresIO(config['postgres-config'])
    postgres.connect()

    bse = BseUtil(config, postgres)
    k_util = KiteUtil(postgres, config)

    session_info = k_util.get_current_session_info()['result'][0]
    kws = KiteTicker(session_info['api_key'], session_info['access_token'])


    def on_ticks(ws, ticks):
        # Callback to receive ticks.
        stock_logger.info("{}".format(json.dumps(ticks)))


    def on_connect(ws, response):
        # Callback on successful connect.
        # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
        instruments = get_instruments_to_fetch()

        ws.subscribe(instruments)

        # Set RELIANCE to tick in `full` mode.
        ws.set_mode(ws.MODE_FULL, instruments)


    def on_close(ws, code, reason):
        # On connection close stop the main loop
        # Reconnection will not happen after executing `ws.stop()`
        ws.stop()


    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    kws.connect()
