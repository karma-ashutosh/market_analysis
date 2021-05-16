class KITE:
    DATA_FILE_READ_BASE_PATH = "/data/kite_websocket_data/historical/"
    DATA_FILE_WRITE_BASE_PATH = "/data/kite_websocket_data/historical/all_pnl_files/"
    SYMBOL = "ADANIPORTS_3861249"
    SYMBOLS = ['ADANIPORTS_3861249', 'ASIANPAINT_60417', 'BAJAJ-AUTO_4267265', 'BAJAJFINSV_4268801',
               'BHARTIARTL_2714625', 'BPCL_134657', 'BRITANNIA_140033', 'CIPLA_177665', 'DIVISLAB_2800641',
               'DRREDDY_225537', 'EICHERMOT_232961', 'GRASIM_315393', 'HCLTECH_1850625', 'HDFC_340481',
               'HDFCBANK_341249', 'HEROMOTOCO_345089', 'HINDPETRO_359937', 'HINDUNILVR_356865', 'INDUSINDBK_1346049',
               'INFY_408065', 'IOC_415745', 'ITC_424961', 'JSWSTEEL_3001089', 'KOTAKBANK_492033', 'LT_2939649',
               'MARUTI_2815745', 'M&M_519937', 'NESTLEIND_4598529', 'NTPC_2977281', 'ONGC_633601', 'POWERGRID_3834113',
               'RELIANCE_738561', 'SAIL_758529', 'SBILIFE_5582849', 'SBIN_779521', 'SHREECEM_794369',
               'SUNPHARMA_857857', 'TATACONSUM_878593', 'TATAMOTORS_884737', 'TECHM_3465729', 'TITAN_897537',
               'ULTRACEMCO_2952193', 'UPL_2889473', 'WIPRO_969473']


class BINANCE:
    # Ashutosh personal key
    # API_KEY = 'WnleOzNJ7wJv06cIExwFM6zs7XzQ3WMvtDyR8smuyHtOUztREwf7wXXsRJ29MXkj'
    # SECRET_KEU = 'tURXrPfdR6BSoD0wwIT4fd4YRVOcnthuHdvNqcL4USsHF2ypagjiJZS6eouqHaO5'

    # Tanmay personal key
    API_KEY = 'fAMfUGJlmezDeQi6EHl86EN3Rdo096kaR4UHUZZd1ilYzXmRQVVSQgvxVdRW31Wj'
    SECRET_KEU = 'AeXANTpERRTR5Yz26n0hiQ9hVxUI8J1QrwUUexuesekYRKFKZQwhb3mjCpbSJlcx'

    COIN = "DOGE"
    CURRENCY = "USDT"
    SYMBOL = COIN + CURRENCY

    DATA_FILE_READ_BASE_PATH = "/data/binance/"
    DATA_FILE_WRITE_BASE_PATH = "/data/binance/"
