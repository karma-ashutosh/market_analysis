class MarketTickEntity:
    # to see the full list of parameters in k_line, visit
    # https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#aggregate-trade-streams
    def __init__(self):
        self.event = None
        self.event_type = None
        self.event_time = None
        self.symbol = None
        self.window_start_epoch_seconds = None
        self.window_end_epoch_seconds = None
        self.interval = None
        self.open = None
        self.close = None
        self.high = None
        self.low = None
        self.volume = None
        self.trade_count = None

    def raw_json(self) -> dict:
        return self.event

    @staticmethod
    def map_from_binance_kline(j_elem: dict):
        result = MarketTickEntity()

        result.raw_event = j_elem

        result.event_type = j_elem.get('e')
        result.event_time = j_elem.get('E')
        result.symbol = j_elem.get('s')

        data = j_elem.get('k')
        result.window_start_epoch_seconds = data.get('t')
        result.window_end_epoch_seconds = int(data.get('T'))
        result.interval = data.get('i')
        result.open = float(data.get('o'))
        result.close = float(data.get('c'))
        result.high = float(data.get('h'))
        result.low = float(data.get('l'))
        result.volume = float(data.get('v'))
        result.trade_count = data.get('n')

        return result

    @staticmethod
    def map_historical_k_line(historical_api_k_line, symbol):
        result = MarketTickEntity()
        j_elem = {
            'e': 'historical',
            's': symbol,
            'k': {
                't': historical_api_k_line[0],
                'o': historical_api_k_line[1],
                'h': historical_api_k_line[2],
                'l': historical_api_k_line[3],
                'c': historical_api_k_line[4],
                'v': historical_api_k_line[5],
                'T': historical_api_k_line[6],
                # number 7 -> Quote asset volume
                'n': historical_api_k_line[8],
                # number 9 -> Taker buy base asset volume
                # number 10 -> Taker buy quote asset volume
                # 11 -> ignore

            }
        }
        result.raw_event = j_elem

        result.event_type = j_elem.get('e')
        # result.symbol = j_elem.get('s')

        data = j_elem.get('k')
        result.window_start_epoch_seconds = data.get('t')
        result.window_end_epoch_seconds = int(data.get('T'))
        result.open = float(data.get('o'))
        result.close = float(data.get('c'))
        result.high = float(data.get('h'))
        result.low = float(data.get('l'))
        result.volume = float(data.get('v'))
        result.trade_count = data.get('n')

        return result

    @staticmethod
    def map_from_kite_event(j_elem: dict):
        pass

