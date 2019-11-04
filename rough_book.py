import pickle
from stock_trade_script import *

if __name__ == '__main__':
    with open("/Users/ashutosh.v/Development/market_analysis_data/analysis_nov/stock.log.2019-11-01_07.pickle", 'rb') \
            as handle:
        ticks_list = pickle.load(handle)

    mc = MainClass()

    for index in range(3000):
        print("index: " + str(index))
        mc.tick(ticks_list[index])
    with open("/Users/ashutosh.v/Development/market_analysis_data/analysis_nov/summary.pickle", 'wb') as handle:
        pickle.dump(mc.get_summary(), handle)

