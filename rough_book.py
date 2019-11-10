import pickle
from datetime import datetime
from kite_enums import Variety, Exchange, TransactionType, PRODUCT, OrderType, VALIDITY
# from stock_trade_script import *
from stock_trade_script import MainClass


def refresh_id(line):
    return line.split(",")[0].replace("refresh_id: ", '')


def action_type(line):
    if 'Starting' in line:
        return 'start'
    return 'end'


def time(line):
    tstr = line.split("at ")[1]
    if "." in tstr:
        tstr = tstr.split(".")[0]
    return datetime.strptime(tstr, '%Y-%m-%d %H:%M:%S')


def process_line(line):
    return tryf(refresh_id, line), tryf(action_type, line), tryf(time, line)


def tryf(func, arg):
    try:
        return func(arg)
    except Exception as e:
        return None


def get_bse_news_crawler_performance():
    lines = [line.strip() for line in open("./bse.log").readlines()]
    processed_lines = [process_line(line) for line in lines]
    result = {}
    for line in processed_lines:
        uid, action, action_time = line[0], line[1], line[2]
        if uid not in result.keys():
            result[uid] = {}

        if action in result[uid].keys():
            raise Exception("duplicate uid action found {} {}".format(uid, action))

        result[uid][action] = action_time

    for v in result.values():
        v['diff'] = v['end'] - v['start']
    return result


if __name__ == '__main__':

    mc = MainClass()
    kite = mc._kite_connect
    kite.place_order(variety=Variety.BRACKET.value,
                     exchange=Exchange.BSE.value,
                     tradingsymbol='ICICIBANK',
                     transaction_type=TransactionType.BUY.value,
                     quantity=1,
                     product=PRODUCT.MIS.value,
                     order_type=OrderType.LIMIT.value,
                     validity=VALIDITY.DAY.value,
                     squareoff=3,
                     stoploss=2.0, trailing_stoploss=1.0, price=470)
