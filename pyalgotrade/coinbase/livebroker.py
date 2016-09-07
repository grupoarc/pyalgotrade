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

import Queue
import datetime

import pyalgotrade.logger
from pyalgotrade import broker
from .netclients import CoinbaseRest as httpclient

btc_symbol = 'BTC'
logger = pyalgotrade.logger.getLogger("coinbase")

class BTCTraits(broker.InstrumentTraits):
    def roundQuantity(self, quantity):
        return round(quantity, 8)

DEFAULT_TRAITS = BTCTraits()

def build_order_from_open_order(openOrder, instrumentTraits=DEFAULT_TRAITS):
    if openOrder.side == 'buy':
        action = broker.Order.Action.BUY
    elif openOrder.side == 'sell':
        action = broker.Order.Action.SELL
    else:
        raise Exception("Invalid order type")
    if 'size' in openOrder:
        size = float(openOrder.size)
    else:
        size = float(openOrder.filled_size)
    if 'price' in openOrder: # limit order
        price = float(openOrder.price)
        ret = broker.LimitOrder(action, btc_symbol, price, size, instrumentTraits)
    else:  # Market order
        onClose = False # TBD
        ret = broker.MarketOrder(action, btc_symbol, size, onClose, instrumentTraits)
    ret.setSubmitted(openOrder.id, openOrder.created_at)
    if 'done_at' not in openOrder:
        if 'filled_size' in openOrder:
            ret.setState(broker.Order.State.PARTIALLY_FILLED)
        else:
            ret.setState(broker.Order.State.ACCEPTED)
    elif 'done_reason' == 'canceled':
        ret.setState(broker.Order.State.CANCELED)
    else:
        ret.setState(broker.Order.State.FILLED)

    return ret


class LiveBroker(broker.Broker):
    """A Bitstamp live broker.

    :param key: API key.
    :type key: string.
    :param secret: API secret.
    :type secret: string.
    :param passphrase: API passphrase.
    :type passphrase: string.


    .. note::
        * Only limit orders are supported.
        * Orders are automatically set as **goodTillCanceled=True** and  **allOrNone=False**.
        * BUY_TO_COVER orders are mapped to BUY orders.
        * SELL_SHORT orders are mapped to SELL orders.
        * API access permissions should include:

          * Account balance
          * Open orders
          * Buy limit order
          * User transactions
          * Cancel order
          * Sell limit order
    """

    QUEUE_TIMEOUT = 0.01

    def __init__(self, key, secret, passphrase, feed):
        super(LiveBroker, self).__init__()
        self.__stop = False
        self.__httpClient = self.buildHTTPClient(key, secret, passphrase)
        self.__tradeMonitor = feed
        self.__cash = 0
        self.__shares = {}
        self.__activeOrders = {}
        self.__userTradeQueue = Queue.Queue()

        feed.getMatchEvent().subscribe(self.onMatchEvent)

    def _registerOrder(self, order):
        assert(order.getId() not in self.__activeOrders)
        assert(order.getId() is not None)
        self.__activeOrders[order.getId()] = order

    def _unregisterOrder(self, order):
        assert(order.getId() in self.__activeOrders)
        assert(order.getId() is not None)
        del self.__activeOrders[order.getId()]

    # Factory method for testing purposes.
    def buildHTTPClient(self, key, secret, passphrase):
        return httpclient(key, secret, passphrase)

    def refreshAccountBalance(self):
        """Refreshes cash and BTC balance."""

        self.__stop = True  # Stop running in case of errors.
        logger.info("Retrieving account balance.")
        balance = self.__httpClient.balances()

        # Cash
        usd = float(balance.get('USD',0))
        self.__cash = round(usd, 2)
        logger.info("%s USD" % (self.__cash))
        # BTC
        btc = float(balance.get('BTC',0))
        self.__shares = {btc_symbol: btc}
        logger.info("%s BTC" % (btc))

        self.__stop = False  # No errors. Keep running.

    def refreshOpenOrders(self):
        self.__stop = True  # Stop running in case of errors.
        logger.info("Retrieving open orders.")
        openOrders = self.__httpClient.Orders(['open', 'pending', 'active'])
        for openOrder in openOrders:
            self._registerOrder(build_order_from_open_order(openOrder, self.getInstrumentTraits(btc_symbol)))

        logger.info("%d open order/s found" % (len(openOrders)))
        self.__stop = False  # No errors. Keep running.

    def _startTradeMonitor(self):
        self.__stop = True  # Stop running in case of errors.
        logger.info("Initializing trade monitor.")
        self.__tradeMonitor.start()
        self.__stop = False  # No errors. Keep running.

    def __fees(self, order, match):
        if type(order) == broker.LimitOrder: return 0
        return 0.0025 * match.price * match.size

    def _onUserTrade(self, match):
        oid = match.involves(self.__activeOrders.keys())
        order = self.__activeOrders.get(oid, None)
        if order is not None:
            self.refreshAccountBalance()
            fee = self.__fees(order, match)
            dt = datetime.fromtimestamp(match.time)
            oei = broker.OrderExecutionInfo(match.price, match.size, fee, dt)
            order.addExecutionInfo(oei)
            # order updated, do housekeeping
            if not order.isActive():
                self._unregisterOrder(order)
            # Notify that the order was updated.
            if order.isFilled():
                eventType = broker.OrderEvent.Type.FILLED
            else:
                eventType = broker.OrderEvent.Type.PARTIALLY_FILLED
            self.notifyOrderEvent(broker.OrderEvent(order, eventType, oei))
            return True
        return False

    # BEGIN observer.Subject interface
    def start(self):
        super(LiveBroker, self).start()
        self.refreshAccountBalance()
        self.refreshOpenOrders()
        self._startTradeMonitor()

    def stop(self):
        self.__stop = True
        logger.info("Shutting down trade monitor.")
        self.__tradeMonitor.stop()

    def join(self):
        if self.__tradeMonitor.isAlive():
            self.__tradeMonitor.join()

    def eof(self):
        return self.__stop

    def dispatch(self):
        evented = False
        # Switch orders from SUBMITTED to ACCEPTED.
        ordersToProcess = self.__activeOrders.values()
        for order in ordersToProcess:
            if order.isSubmitted():
                order.switchState(broker.Order.State.ACCEPTED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None))
                evented = True
        # Handle a user trade, if any
        try:
            match = self.__userTradeQueue.get(True, LiveBroker.QUEUE_TIMEOUT)
            utraded = self._onUserTrade(match)
            evented = utraded or evented
        except Queue.Empty:
            pass
        return evented

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # END observer.Subject interface

    def onMatchEvent(self, match):
        if match.involves(self.__activeOrders.keys()):
            self.__userTradeQueue.put(match)


    # BEGIN broker.Broker interface

    def getCash(self, includeShort=True):
        return self.__cash

    def getInstrumentTraits(self, instrument):
        return BTCTraits()

    def getShares(self, instrument):
        return self.__shares.get(instrument, 0)

    def getPositions(self):
        return self.__shares

    def getActiveOrders(self, instrument=None):
        return self.__activeOrders.values()

    def submitOrder(self, order):
        if order.isInitial():
            # Override user settings based on Bitstamp limitations.
            order.setAllOrNone(False)
            order.setGoodTillCanceled(True)

            side = "buy" if order.isBuy() else "sell"
            size = order.getQuantity()
            if order.getType() == order.Type.LIMIT:
                price = order.getLimitPrice()
                flags = (httpclient.POST_ONLY, httpclient.GTC)
                newOrderId = self.__httpClient.limitorder(side, price, size, flags=flags)
            elif order.getType() == order.Type.MARKET:
                newOrderId = self.__httpClient.marketorder(side, size)
            else:
                raise Exception("Coinbase only does LIMIT and MARKET orders")

            tries = 0
            newOrder = None
            while newOrder is None and tries < 5:
                try:
                    newOrder = self.__httpClient.Order(newOrderId)
                except Exception:
                    pass
                tries += 1

            if newOrder is None:
                raise Exception("Unable to get status of coinbase order %s" % newOrderId)

            order.setSubmitted(newOrderId, newOrder.created_at)
            self._registerOrder(order)
            # Switch from INITIAL -> SUBMITTED
            # IMPORTANT: Do not emit an event for this switch because when using the position interface
            # the order is not yet mapped to the position and Position.onOrderUpdated will get called.
            order.switchState(broker.Order.State.SUBMITTED)
        else:
            raise Exception("The order was already processed")

    def _createOrder(self, orderType, action, instrument, quantity, price):

        if instrument != btc_symbol:
            raise Exception("Only BTC instrument is supported")

        action = {
            broker.Order.Action.BUY_TO_COVER: broker.Order.Action.BUY,
            broker.Order.Action.BUY:          broker.Order.Action.BUY,
            broker.Order.Action.SELL_SHORT:   broker.Order.Action.SELL,
            broker.Order.Action.SELL:         broker.Order.Action.SELL
        }.get(action, None)
        if action is None:
            raise Exception("Only BUY/SELL orders are supported")

        instrumentTraits = self.getInstrumentTraits(instrument)
        quantity = instrumentTraits.roundQuantity(quantity)
        price = round(price, 2)
        if orderType == broker.MarketOrder:
            return orderType(action, instrument, quantity, False, instrumentTraits)
        elif orderType == broker.LimitOrder:
            return orderType(action, instrument, price, quantity, instrumentTraits)

    def createMarketOrder(self, action, instrument, quantity, onClose=False):
        return self._createOrder(broker.MarketOrder, action, instrument, quantity, 0.)

    def createLimitOrder(self, action, instrument, limitPrice, quantity):
        return self._createOrder(broker.LimitOrder, action, instrument, quantity, limitPrice)

    def createStopOrder(self, action, instrument, stopPrice, quantity):
        raise Exception("Stop orders are not supported")

    def createStopLimitOrder(self, action, instrument, stopPrice, limitPrice, quantity):
        raise Exception("Stop limit orders are not supported")

    def cancelOrder(self, order):
        activeOrder = self.__activeOrders.get(order.getId())
        if activeOrder is None:
            raise Exception("The order is not active anymore")
        if activeOrder.isFilled():
            raise Exception("Can't cancel order that has already been filled")

        self.__httpClient.cancel(order.getId())
        self._unregisterOrder(order)
        order.switchState(broker.Order.State.CANCELED)

        # Update cash and shares.
        self.refreshAccountBalance()

        # Notify that the order was canceled.
        self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.CANCELED, "User requested cancellation"))

    # END broker.Broker interface
