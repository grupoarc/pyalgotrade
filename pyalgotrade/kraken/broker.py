# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

import threading
from datetime import datetime

from pyalgotrade import bar
from pyalgotrade import broker
from pyalgotrade.broker import backtesting, MarketOrder
from pyalgotrade.coinbase import common
from pyalgotrade.kraken import livebroker
from pyalgotrade.orderbook import OrderBook, Bid, Ask


LiveBroker = livebroker.LiveBroker

# In a backtesting or paper-trading scenario the BacktestingBroker dispatches events while processing events from the
# BarFeed.
# It is guaranteed to process BarFeed events before the strategy because it connects to BarFeed events before the
# strategy.

class CoinbaseFees(backtesting.Commission):

    def calculate(self, order, price, quantity):
        if type(order) == MarketOrder:
            return 0.0025 * price * quantity
        return 0

class BacktestingBroker(backtesting.Broker):
    MIN_TRADE_USD = 5

    """A Coinbase backtesting broker.

    :param cash: The initial amount of cash.
    :type cash: int/float.
    :param barFeed: The bar feed that will provide the bars.
    :type barFeed: :class:`pyalgotrade.barfeed.BarFeed`
    :param fee: The fee percentage for each order. Defaults to 0.25%.
    :type fee: float.

    .. note::
        * Only limit orders are supported.
        * Orders are automatically set as **goodTillCanceled=True** and  **allOrNone=False**.
        * BUY_TO_COVER orders are mapped to BUY orders.
        * SELL_SHORT orders are mapped to SELL orders.
    """

    def __init__(self, cash, barFeed):
        commission = CoinbaseFees()
        super(BacktestingBroker, self).__init__(cash, barFeed, commission)
        self.__book = OrderBook()
        barFeed.getOrderBookUpdateEvent().subscribe(self.__book.update)
        self.__barFeed = barFeed
        self._Broker__shares['BTC'] = cash

    def getInstrumentTraits(self, instrument):
        return common.BTCTraits()

    def submitOrder(self, order):
        if order.isInitial():
            # Override user settings based on Bitstamp limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)

        threading.Timer(1, self._simulateTrade, args=[order]).start()

        return super(BacktestingBroker, self).submitOrder(order)

    def _simulateTrade(self, order):
        bars = self._fakeBarsForOrder(order)
        if not bars: return
        self.__barFeed.getNewValuesEvent().emit(bars.getDateTime(), bars)

    def _fakeBarsForOrder(self, order):
        volume = order.getQuantity()
        if order.getAction() in [ broker.Order.Action.BUY, broker.Order.Action.BUY_TO_COVER ]:
            side, dir_ = Ask, bar.TradeBar.UP
        else:
            side, dir_ = Bid, bar.TradeBar.DOWN
        open_ = high = low = close = self.__book.price_for_size(side, volume) / volume
        if not high: return
        adjClose = None
        freq = bar.Frequency.TRADE
        time = datetime.now()
        fakebar = bar.TradeBar(time, open_, high, low, close, volume, adjClose, freq, dir_)
        return bar.Bars({'BTC':fakebar})

    def _check_order(self, action, instrument, quantity, totalprice):
        if totalprice < BacktestingBroker.MIN_TRADE_USD:
            raise Exception("Trade must be >= %s" % (BacktestingBroker.MIN_TRADE_USD))
        if action == broker.Order.Action.BUY:
            if totalprice is None: return
            if totalprice > self.getCash(False):
                raise Exception("Not enough cash")
        elif action == broker.Order.Action.SELL:
            if quantity > self.getShares(instrument):
                raise Exception("Not enough %s" % (common.btc_symbol))
        else:
            raise Exception("Only BUY/SELL orders are supported")

    def _remap_action(self, action):
        action = {
            broker.Order.Action.BUY_TO_COVER: broker.Order.Action.BUY,
            broker.Order.Action.BUY:          broker.Order.Action.BUY,
            broker.Order.Action.SELL_SHORT:   broker.Order.Action.SELL,
            broker.Order.Action.SELL:         broker.Order.Action.SELL
        }.get(action, None)
        if action is None:
            raise Exception("Only BUY/SELL orders are supported")
        return action

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
       action = self._remap_action(action)
       side = Bid if action == broker.Order.Action.BUY else Ask
       # where do we get a book?
       totalprice = self.__book.price_for_size(side, quantity)
       self._check_order(action, instrument, quantity, totalprice)
       return super(BacktestingBroker, self).createMarketOrder(action, instrument, quantity, onClose)

    def createLimitOrder(self, action, instrument, limitPrice, quantity):

        action = self._remap_action(action)

        if limitPrice * quantity < BacktestingBroker.MIN_TRADE_USD:
            raise Exception("Trade must be >= %s" % (BacktestingBroker.MIN_TRADE_USD))

        fee = self.getCommission().calculate(None, limitPrice, quantity)
        self._check_order(action, instrument, quantity, limitPrice * quantity + fee)

        return super(BacktestingBroker, self).createLimitOrder(action, instrument, limitPrice, quantity)

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        raise Exception("Stop orders are not supported")

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        raise Exception("Stop limit orders are not supported")


class PaperTradingBroker(BacktestingBroker):
    """A Bitstamp paper trading broker.

    :param cash: The initial amount of cash.
    :type cash: int/float.
    :param barFeed: The bar feed that will provide the bars.
    :type barFeed: :class:`pyalgotrade.barfeed.BarFeed`
    :param fee: The fee percentage for each order. Defaults to 0.5%.
    :type fee: float.

    .. note::
        * Only limit orders are supported.
        * Orders are automatically set as **goodTillCanceled=True** and  **allOrNone=False**.
        * BUY_TO_COVER orders are mapped to BUY orders.
        * SELL_SHORT orders are mapped to SELL orders.
    """

    pass
