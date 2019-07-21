import json
from datetime import datetime
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
    # instruments = get_instruments_to_fetch()
    instruments = [138127364, 136354052, 137872900, 136313604, 2753281, 137910532, 130866180, 149249, 136178180,
                   129637124, 189185, 135895812, 4741121, 130834436, 215553, 135975684, 128168964, 295169, 134848260,
                   136503556, 4918017, 138541828, 637185, 138041604, 137125892, 7670273, 128062724, 128063236, 492033,
                   128064516, 506625, 134068228, 543745, 135722756, 138034948, 128080644, 636673, 132485636, 137957636,
                   131482884, 3821313, 136417028, 136279812, 2170625, 136302596, 129385476, 1084161]


    def on_ticks(ws, ticks):
        # Callback to receive ticks.
        for tick in ticks:
            for key in tick.keys():
                if isinstance(tick[key], datetime):
                    tick[key] = str(tick[key])

        stock_logger.info("{}".format(json.dumps(ticks)))


    def on_connect(ws, response):
        # Callback on successful connect.
        # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).

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
