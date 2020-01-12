from abc import abstractmethod

from constants import LAST_PRICE
from kite_enums import TransactionType


def event_to_price(event):
    return event[LAST_PRICE]


class ExitStrategy:
    def __init__(self, transaction_type: TransactionType):
        self._transaction_type = transaction_type
        self._profit_breached = False
        self._loss_breached = False

    def consume_event(self, market_event):
        event_price = event_to_price(market_event)
        if self._transaction_type == TransactionType.LONG:
            self._update_values_for_long(event_price)
        else:
            self._update_values_for_short(event_price)

    def profit_breached(self):
        self._profit_breached = True

    def loss_breached(self):
        self._loss_breached = True

    def should_exit(self):
        return self._profit_breached or self._loss_breached

    @abstractmethod
    def _update_values_for_long(self, event_price):
        pass

    @abstractmethod
    def _update_values_for_short(self, event_price):
        pass


class PercentageMovingStopLossExitStrategy(ExitStrategy):
    def __init__(self, transaction_type: TransactionType, first_event, stop_loss_threshold_percent,
                 stop_loss_update_threshold_percent):
        super().__init__(transaction_type)
        self._last_favourable_price = event_to_price(first_event)
        self._stop_loss_threshold = 1.0 * stop_loss_threshold_percent * self._last_favourable_price / 100
        self._stop_loss_update_threshold = 1.0 * stop_loss_update_threshold_percent * self._last_favourable_price / 100

    def _update_values_for_short(self, event_price):
        if event_price <= self._last_favourable_price - self._stop_loss_update_threshold:
            self._last_favourable_price = event_price
        elif event_price >= self._last_favourable_price + self._stop_loss_threshold:
            self.loss_breached()

    def _update_values_for_long(self, event_price):
        if event_price >= self._last_favourable_price + self._stop_loss_update_threshold:
            self._last_favourable_price = event_price
        elif event_price <= self._last_favourable_price - self._stop_loss_threshold:
            self.loss_breached()


class AbsoluteValueExitStrategy(ExitStrategy):
    def __init__(self, transaction_type: TransactionType, first_event, profit_margin_limit_percent,
                 loss_margin_limit_percent):
        super().__init__(transaction_type)
        self._entry_price = event_to_price(first_event)
        self._profit_margin_limit = 1.0 * profit_margin_limit_percent * self._entry_price / 100
        self._loss_margin_limit = 1.0 * loss_margin_limit_percent * self._entry_price / 100

    def _update_values_for_long(self, event_price):
        price_diff = event_price - self._entry_price
        if price_diff > 0 and price_diff > self._profit_margin_limit:
            self.profit_breached()
        elif price_diff < 0 and price_diff * -1 > self._loss_margin_limit:
            self.loss_breached()

    def _update_values_for_short(self, event_price):
        price_diff = event_price - self._entry_price
        if price_diff < 0 and price_diff * -1 > self._profit_margin_limit:
            self.profit_breached()
        elif price_diff > 0 and price_diff > self._loss_margin_limit:
            self.loss_breached()


class ExitStrategyFactory:
    def __init__(self, exit_config_dict):
        self._exit_config = exit_config_dict
        self._trailing_stoploss_strategy = 'trailing_stoploss'
        self._absolute_strategy = 'absolute'
        self._loss_margin_limit_key = 'loss_margin_limit_percent'
        self._profit_margin_limit_key = 'profit_margin_limit_percent'
        self._stop_loss_update_threshold_key = 'stop_loss_update_threshold_percent'
        self._stop_loss_threshold_key = 'stop_loss_threshold_percent'

        self._strategy = exit_config_dict['strategy']
        self.__validate_keys(exit_config_dict)

    def __validate_keys(self, config_dict: dict):
        if self._strategy == self._absolute_strategy:
            req_keys = [self._profit_margin_limit_key, self._loss_margin_limit_key]
        elif self._strategy == self._trailing_stoploss_strategy:
            req_keys = [self._stop_loss_threshold_key, self._stop_loss_update_threshold_key]
        else:
            raise Exception("strategy has to be either 'absolute' or 'trailing_stoploss'")

        if not(all([key in config_dict.keys() for key in req_keys])):
            raise Exception("Keys: {} have to be provided for strategy: {}".format(req_keys, self._strategy))

    def exit_strategy(self, transaction_type: TransactionType, first_event: dict) -> ExitStrategy:
        if self._strategy == self._absolute_strategy:
            return AbsoluteValueExitStrategy(transaction_type, first_event,
                                             self._exit_config[self._profit_margin_limit_key],
                                             self._exit_config[self._loss_margin_limit_key])
        elif self._strategy == self._trailing_stoploss_strategy:
            return PercentageMovingStopLossExitStrategy(transaction_type, first_event,
                                                        self._exit_config[self._stop_loss_threshold_key],
                                                        self._exit_config[self._stop_loss_update_threshold_key])
