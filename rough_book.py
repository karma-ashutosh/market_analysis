import pickle
from stock_trade_script import *

if __name__ == '__main__':
    with open("/Users/ashutosh.v/Development/ticks.pickle", 'rb') as handle:
        ticks = pickle.load(handle)

    tick = ticks[0]

    mc = MainClass()
    mc.tick(tick)
