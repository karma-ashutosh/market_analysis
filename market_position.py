import pickle
from abc import abstractmethod
from logging import Logger
from general_util import create_dir_if_not_exists
from constants import LAST_PRICE
from exit_strategy import ExitStrategy, ExitStrategyFactory
from kite_enums import TransactionType
from datetime import datetime

logger = None


def set_market_position_logger(target: Logger):
    global logger
    logger = target


class MarketPosition:
    def __init__(self, exit_strategy_factory: ExitStrategyFactory):
        self._exit_strategy_factory = exit_strategy_factory
        self._entry_scores = None
        self._entry_event = None
        self._exit_event = None
        self._transaction_type = None
        self._took_position = False
        self._trade_completed = False
        self._entry_price = None
        self._exit_strategy: ExitStrategy = None

    @staticmethod
    def __price(event):
        return event[LAST_PRICE]

    def consume_event(self, market_event):
        self._exit_strategy.consume_event(market_event)

    def enter(self, entry_event, transaction_type: TransactionType, scores: list):
        self._entry_event = entry_event
        self._entry_price = self.__price(entry_event)
        self._transaction_type = transaction_type
        self._entry_scores = scores
        self._took_position = True
        self._exit_strategy = self._exit_strategy_factory.exit_strategy(transaction_type, entry_event)
        msg = "buy" if transaction_type == TransactionType.LONG else "sell"
        logger.info("{} stocks at : ".format(msg) + str(entry_event))

    def entry_price(self):
        return self._entry_event[LAST_PRICE]

    def exit(self, exit_event):
        self._exit_event = exit_event
        self._trade_completed = True

    def is_trade_done(self):
        return self._trade_completed

    def should_exit(self):
        return self._exit_strategy.should_exit()

    def enter_transaction_type(self):
        return self._transaction_type

    def get_summary(self):
        return {
            'entry': self._entry_event,
            'exit': self._exit_event,
            'score': self._entry_scores,
            'type': self._transaction_type.description if self._transaction_type else None
        }


class MarketPositionFactory:
    def __init__(self, exit_strategy_factory: ExitStrategyFactory):
        self._exit_strategy_factory = exit_strategy_factory

    def get_market_position(self, exchange, identifier, date: datetime) -> MarketPosition:
        key = self._market_pos_key(date, exchange, identifier)
        existing_market_pos = self._get_existing_market_position(key)
        if existing_market_pos:
            return existing_market_pos
        new_market_pos = MarketPosition(self._exit_strategy_factory)
        self._persist_market_position(key, new_market_pos)
        return new_market_pos

    @staticmethod
    def _market_pos_key(date, exchange, identifier):
        key = ":".join([exchange, identifier, str(date.date())])
        return key

    def persist(self, exchange, identifier, date: datetime, market_position: MarketPosition):
        key = self._market_pos_key(date, exchange, identifier)
        self._persist_market_position(key, market_position)

    @abstractmethod
    def _get_existing_market_position(self, key: str) -> MarketPosition:
        pass

    @abstractmethod
    def _persist_market_position(self, key: str, market_position: MarketPosition):
        pass


class FileBaseMarketPositionFactory(MarketPositionFactory):
    def __init__(self, exit_strategy_factory: ExitStrategyFactory, file_path):
        super().__init__(exit_strategy_factory)
        try:
            with open(file_path, 'rb') as handle:
                self._position_dict = pickle.load(handle)
        except Exception as e:
            logger.error("Error while loading position dict: {}".format(file_path), e)
            self._position_dict = {}
            create_dir_if_not_exists(file_name=file_path)
        self._dict_pickle_path = file_path

    def _get_existing_market_position(self, key: str) -> MarketPosition:
        return self._position_dict.get(key)

    def _persist_market_position(self, key: str, market_position: MarketPosition):
        self._position_dict[key] = market_position
        with open(self._dict_pickle_path, 'wb') as handle:
            pickle.dump(self._position_dict, handle)
        pickle.dumps(self._position_dict)
