import pickle
from datetime import datetime

from stock_trade_script import *


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


def get_performance():
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
    with open("/Users/ashutosh.v/Development/market_analysis_data/analysis_nov/stock.log.2019-11-01_07.pickle", 'rb') \
            as handle:
        ticks_list = pickle.load(handle)

    mc = MainClass()

    for index in range(3000):
        print("index: " + str(index))
        mc.handle_ticks_safely(ticks_list[index])
    with open("/Users/ashutosh.v/Development/market_analysis_data/analysis_nov/summary.pickle", 'wb') as handle:
        pickle.dump(mc.get_summary(), handle)
