import json
from datetime import datetime
import yaml
from kiteconnect import KiteTicker
from kiteconnect import KiteConnect

from alerts import Alert
from kite_util import KiteUtil
from general_util import setup_logger
from postgres_io import PostgresIO

stock_logger = setup_logger("stock_logger", "/data/kite_websocket_data/stock.log", msg_only=True)
msg_logger = setup_logger("msg_logger", "/tmp/app.log")


def get_instruments_to_fetch():
    # results_for_today = bse.get_result_announcement_meta_for_today()
    # results_for_yesterday = bse.get_result_announcement_meta_for_yesterday()
    # results_for_today.extend(results_for_yesterday)
    #
    # bse_security_codes = list(map(lambda j: j['security_code'], results_for_today))
    # nse_security_codes = list(k_util.get_nse_exchange_token_for_bse_exchange_token(bse_security_codes).values())
    #
    # bse_instrument_mapping = k_util.map_bse_code_to_instrument_id(bse_security_codes)
    # nse_instrument_mapping = k_util.map_nse_code_to_instrument_id(nse_security_codes)
    # instrument_tokens = [int(v) for v in bse_instrument_mapping.values()]
    # instrument_tokens.extend([int(v) for v in nse_instrument_mapping.values()])
    # msg_logger.info("logging instrument tokens: "+str(instrument_tokens))
    # msg_logger.info("logging bse_security_codes: "+str(bse_security_codes))
    # msg_logger.info("logging nse_security_codes: "+str(nse_security_codes))
    # return instrument_tokens
    return [256265, 260105, 17057282, 17058050, 17060610, 17061890, 17064706, 17064962, 11207170, 11207426, 11217666,
            11217922, 11226370, 11226626, 13628930, 13630722, 13652994, 13653250, 13665026, 13665282, 15409154,
            15409410, 15411202, 15411714, 15418114, 15419394, 15933698, 15933954, 15937282, 15937538, 15943426,
            15943682, 10810370, 10810626, 10817026, 10817282, 10825474, 10825730, 8962306, 8962562, 9459458, 9459714,
            9469698, 9469954, 15027970, 15028226, 10014978, 10015234, 17640962, 17641218, 17657090, 17657346, 18061570,
            18062850, 18065154, 18065410, 18784514, 18784770, 18787074, 18787330, 19098370, 19098626, 10268418,
            10268674, 20565762, 20566018, 20568322, 20568578, 21607938, 21608194, 21609218, 21609474, 12800514,
            12800770, 12797954, 12798210, 23747074, 23747330, 23842562, 23842818, 12279554, 12280322, 12282114,
            12282370, 28533762, 28534018, 28536322, 28536578, 28874754, 28875010, 28877314, 28877570, 28976642,
            28976898, 9869058, 9869826, 29199874, 29200130, 29202434, 29202690, 29222402, 29222658, 29224450, 29224706,
            29384194, 29384450, 29386754, 29387010, 16492034, 12418050, 15670018, 16491522, 12417538, 15665922,
            16492802, 16493058, 16494594, 16497154, 16498690, 15706882, 16563970, 16567810, 16568066, 16580354,
            16581890, 16592386, 16593666, 16616962, 16617218, 16629250, 6401, 912129, 3861249, 4451329, 60417, 1195009,
            112129, 5105409, 3677697, 2393089, 415745, 424961, 3924993, 633601, 738561, 779521, 878593, 884737, 897537]


if __name__ == '__main__':
    with open('./config.yml') as handle:
        config = yaml.load(handle)
    postgres = PostgresIO(config['postgres-config'])
    # postgres.connect()

    # bse = BseUtil(config, postgres)
    k_util = KiteUtil(postgres, config)

    session_info = k_util.get_current_session_info()['result'][0]
    instruments = get_instruments_to_fetch()

    kws = KiteTicker(session_info['api_key'], session_info['access_token'])
    kite = KiteConnect(session_info['api_key'], session_info['access_token'])

    alert = Alert(config)


    def on_ticks(ws, ticks):
        # Callback to receive ticks.
        alert.send_heartbeat("kite_web_socket_streaming")
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
