# Kite Tick Keys ####

EMPTY_KEY = ''

TIMESTAMP = 'timestamp'

LAST_PRICE = 'last_price'

INSTRUMENT_TOKEN = 'instrument_token'

LAST_TRADE_TIME = 'last_trade_time'

SELL_QUANTITY = 'sell_quantity'

BUY_QUANTITY = 'buy_quantity'

VOLUME = 'volume'

KITE_EVENT_DATETIME_OBJ = 'datetime'

# Can not recall which keys

BASE_DIR = "/Users/ashutosh.v/Development/market_analysis_data"


# Kite Moving Avg Constants

class MovingAvgConstants:
    FILE_NAMES = []


class TextFileConstants:
    KITE_INSTRUMENTS = "/home/karma/Development/market_analysis/text_files/instruments.csv"
    NIFTY_50_INSTRUMENTS = "text_files/nifty_instruments.tsv"
    KITE_DATA_BASE_DIR = "/data/kite_websocket_data/"
    KITE_HISTORICAL_BASE_DIR = KITE_DATA_BASE_DIR + "historical/"
    KITE_CURRENT_DATA = KITE_DATA_BASE_DIR + "current_daily_data/"
    NIFTY_50_DATA_FILE_NAMES = ['ADANIPORTS_3861249.json', 'ASIANPAINT_60417.json', 'BAJAJ-AUTO_4267265.json',
                                'BAJAJFINSV_4268801.json',
                                'BHARTIARTL_2714625.json', 'BPCL_134657.json', 'BRITANNIA_140033.json',
                                'CIPLA_177665.json',
                                'DIVISLAB_2800641.json', 'DRREDDY_225537.json', 'EICHERMOT_232961.json',
                                'GRASIM_315393.json',
                                'HCLTECH_1850625.json', 'HDFC_340481.json', 'HDFCBANK_341249.json',
                                'HEROMOTOCO_345089.json',
                                'HINDPETRO_359937.json', 'HINDUNILVR_356865.json', 'INDUSINDBK_1346049.json',
                                'INFY_408065.json',
                                'IOC_415745.json', 'ITC_424961.json', 'JSWSTEEL_3001089.json', 'KOTAKBANK_492033.json',
                                'LT_2939649.json',
                                'MARUTI_2815745.json', 'M&M_519937.json', 'NESTLEIND_4598529.json', 'NTPC_2977281.json',
                                'ONGC_633601.json',
                                'POWERGRID_3834113.json', 'RELIANCE_738561.json', 'SAIL_758529.json',
                                'SBILIFE_5582849.json',
                                'SBIN_779521.json',
                                'SHREECEM_794369.json', 'SUNPHARMA_857857.json', 'TATACONSUM_878593.json',
                                'TATAMOTORS_884737.json',
                                'TECHM_3465729.json', 'TITAN_897537.json', 'ULTRACEMCO_2952193.json',
                                'UPL_2889473.json',
                                'WIPRO_969473.json']


class URLS:
    KITE_SHARE_HISTORY_URL_FORMAT = "https://api.kite.trade/instruments/historical/{}/{}?from={}&to={}"

    
class BINANCE:
    API_KEY = 'WnleOzNJ7wJv06cIExwFM6zs7XzQ3WMvtDyR8smuyHtOUztREwf7wXXsRJ29MXkj'
    SECRET_KEU = 'tURXrPfdR6BSoD0wwIT4fd4YRVOcnthuHdvNqcL4USsHF2ypagjiJZS6eouqHaO5'
