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
import time
import Queue

import pyalgotrade.logger
from pyalgotrade import broker
from .netclients import RESTClient as httpclient

btc_symbol = 'BTC'
logger = pyalgotrade.logger.getLogger("bitfinex")

class BTCTraits(broker.InstrumentTraits):
    def roundQuantity(self, quantity):
        return round(quantity, 8)


def build_order_from_open_order(openOrder, instrumentTraits):
    if openOrder.side == 'buy':
        action = broker.Order.Action.BUY
    elif openOrder.side == 'sell':
        action = broker.Order.Action.SELL
    else:
        raise Exception("Invalid order type")

    price = float(openOrder.price)
    size = float(openOrder.size)
    ret = broker.LimitOrder(action, btc_symbol, price, size, instrumentTraits)
    ret.setSubmitted(openOrder.id, openOrder.created_at)
    ret.setState(broker.Order.State.ACCEPTED)
    return ret


class TradeMonitor(threading.Thread):
    POLL_FREQUENCY = 2

    # Events
    ON_USER_TRADE = 1

    def __init__(self, httpClient):
        super(TradeMonitor, self).__init__()
        self.__lastTradeAt = None
        self.__httpClient = httpClient
        self.__queue = Queue.Queue()
        self.__running = True

    def _getNewTrades(self):
        trades = self.__httpClient.orders()
        if trades: return []
        if self.__lastTradeAt is None:
            self.__lastTradeAt = max(trades, key=lambda k: k['created_at'])
        else:
            trades = [ t for t in trades if t['created_at'] > self.__lastTradeAt ]
        return trades

    def getQueue(self):
        return self.__queue

    def start(self):
        trades = self._getNewTrades()
        # Store the last trade id since we'll start processing new ones only.
        if len(trades):
            logger.info("Last trade found: %d" % (self.__lastTradeId))
        super(TradeMonitor, self).start()

    def run(self):
        while self.__running:
            try:
                trades = self._getNewTrades()
                if trades:
                    logger.info("%d new trade/s found" % (len(trades)))
                    self.__queue.put((TradeMonitor.ON_USER_TRADE, trades))
            except Exception, e:
                logger.critical("Error retrieving user transactions", exc_info=e)

            time.sleep(TradeMonitor.POLL_FREQUENCY)

    def stop(self):
        self.__running = False


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

    def __init__(self, key, secret, passphrase):
        super(LiveBroker, self).__init__()
        self.__stop = False
        self.__httpClient = self.buildHTTPClient(key, secret)
        self.__tradeMonitor = TradeMonitor(self.__httpClient)
        self.__cash = 0
        self.__shares = {}
        self.__activeOrders = {}

    def _registerOrder(self, order):
        assert(order.getId() not in self.__activeOrders)
        assert(order.getId() is not None)
        self.__activeOrders[order.getId()] = order

    def _unregisterOrder(self, order):
        assert(order.getId() in self.__activeOrders)
        assert(order.getId() is not None)
        del self.__activeOrders[order.getId()]

    # Factory method for testing purposes.
    def buildHTTPClient(self, key, secret):
        return httpclient(key, secret)

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

    def _onUserTrades(self, trades):
        for trade in trades:
            order = self.__activeOrders.get(trade.getOrderId())
            if order is not None:
                fee = trade.getFee()
                fillPrice = trade.getBTCUSD()
                btcAmount = trade.getBTC()
                dateTime = trade.getDateTime()

                # Update cash and shares.
                self.refreshAccountBalance()
                # Update the order.
                orderExecutionInfo = broker.OrderExecutionInfo(fillPrice, abs(btcAmount), fee, dateTime)
                order.addExecutionInfo(orderExecutionInfo)
                if not order.isActive():
                    self._unregisterOrder(order)
                # Notify that the order was updated.
                if order.isFilled():
                    eventType = broker.OrderEvent.Type.FILLED
                else:
                    eventType = broker.OrderEvent.Type.PARTIALLY_FILLED
                self.notifyOrderEvent(broker.OrderEvent(order, eventType, orderExecutionInfo))
            else:
                logger.info("Trade %d refered to order %d that is not active" % (trade.getId(), trade.getOrderId()))

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
        # Switch orders from SUBMITTED to ACCEPTED.
        ordersToProcess = self.__activeOrders.values()
        for order in ordersToProcess:
            if order.isSubmitted():
                order.switchState(broker.Order.State.ACCEPTED)
                self.notifyOrderEvent(broker.OrderEvent(order, broker.OrderEvent.Type.ACCEPTED, None))

        # Dispatch events from the trade monitor.
        try:
            eventType, eventData = self.__tradeMonitor.getQueue().get(True, LiveBroker.QUEUE_TIMEOUT)

            if eventType == TradeMonitor.ON_USER_TRADE:
                self._onUserTrades(eventData)
            else:
                logger.error("Invalid event received to dispatch: %s - %s" % (eventType, eventData))
        except Queue.Empty:
            pass

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # END observer.Subject interface

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
            price = order.getLimitPrice()
            size =order.getQuantity()
            newOrderId = self.__httpClient.limitorder(side, price, size)
            newOrder = self.__httpClient.Order(newOrderId)

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
            broker.Order.Action.BUY_TO_COVER: broker.Order.action.BUY,
            broker.Order.Action.BUY:          broker.Order.action.BUY,
            broker.Order.Action.SELL_SHORT:   broker.Order.action.SELL,
            broker.Order.Action.SELL:         broker.Order.action.SELL
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
