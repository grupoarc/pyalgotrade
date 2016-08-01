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

import json
from datetime import datetime
import threading
import Queue

from pyalgotrade import bar
from pyalgotrade.websocket.client import WebSocketClientBase
from pyalgotrade.bitfinex import common
from pyalgotrade.bitfinex.book import Book, MarketUpdate

from pyalgotrade.bitfinex.netclients import toMarketMessage


def get_current_datetime():
    return datetime.now()


class TradeBar(bar.Bar):
    # Optimization to reduce memory footprint.
    __slots__ = ('__dateTime', '__tradeId', '__price', '__amount', '__buy')

    def __init__(self, dateTime, tradeId, price, amount, isBuy):
        self.__dateTime = dateTime
        self.__tradeId = tradeId
        self.__price = price
        self.__amount = amount
        self.__buy = isBuy

    def __setstate__(self, state):
        (self.__dateTime, self.__tradeId, self.__price, self.__amount, self.__buy) = state

    def __getstate__(self):
        return (self.__dateTime, self.__tradeId, self.__price, self.__amount, self.__buy)

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted:
            raise Exception("Adjusted close is not available")

    def getTradeId(self):
        return self.__tradeId

    def getFrequency(self):
        return bar.Frequency.TRADE

    def getDateTime(self):
        return self.__dateTime

    def setDateTime(self, value):
        self.__dateTime = value

    def getOpen(self, adjusted=False):
        return self.__price

    def getHigh(self, adjusted=False):
        return self.__price

    def getLow(self, adjusted=False):
        return self.__price

    def getClose(self, adjusted=False):
        return self.__price

    def getVolume(self):
        return self.__amount

    def getAdjClose(self):
        return None

    def getTypicalPrice(self):
        return self.__price

    def getPrice(self):
        return self.__price

    def getUseAdjValue(self):
        return False

    def isBuy(self):
        return self.__buy

    def isSell(self):
        return not self.__buy

    EPOCH = datetime(1970,1,1)

    @classmethod
    def fromCoinbaseMatch(cls, match):
        time = (datetime.strptime(match['time'], "%Y-%m-%dT%H:%M:%S.%fZ") - cls.EPOCH).total_seconds()
        isBuy = match['side'] == 'buy'
        return cls(time, match['trade_id'], float(match['price']), float(match['size']), isBuy)

    @classmethod
    def fromBitfinexTrade(cls, t):
        ttype = t[0]
        if ttype == 'te':
            seq, timestamp, price, size = t[1:]
        elif ttype == 'tu':
            seq, tid, timestamp, price, size = t[1:]
        else:
            common.logger.error("unknown bitfinex trade type: " + repr(t))
            raise SyntaxError("unknown bitfinex trade type: " + repr(t))
        size = float(size)
        return cls(float(timestamp), tid, float(price), abs(size), size>0)


class WebSocketClient(WebSocketClientBase):

    # Events
    ON_CONNECTED = object()
    ON_DISCONNECTED = object()
    ON_TRADE = object()
    ON_ORDER_BOOK_UPDATE = object()

    def __init__(self):
        url = "wss://api2.bitfinex.com:3000/ws"
        super(WebSocketClient, self).__init__(url)
        self.__queue = Queue.Queue()
        self._book = Book("bitfinex", "BTCUSD")
        self._channel = {}
        self.__lastTradeTime = 0.0

    def getQueue(self):
        return self.__queue


    ######################################################################
    # WebSocketClientBase events.

    def onOpened(self):
        self.__queue.put((WebSocketClient.ON_CONNECTED, None))
        common.logger.info("Connected; subscribing.")
        subscribe_book = json.dumps({"event": "subscribe",
                                     "channel": "book",
                                     "pair": "BTCUSD",
                                     "freq": "F0",
                                     })
        subscribe_trades = json.dumps({"event": "subscribe",
                                       "channel": "trades",
                                       "pair": "BTCUSD",
                                       })
        self.send(subscribe_book + '\n')
        self.send(subscribe_trades + '\n')
        common.logger.info("Connected; subscribed.")

    def onMessage(self, m):
        if type(m) == dict:
            if m['event'] == 'info':
                common.logger.info("bitfinex ws info:" + repr(m))
            elif m['event'] == 'error':
                common.logger.error("bitfinex ws error: " + repr(m))
            elif m['event'] == 'subscribed':
                common.logger.info("got subscription" + repr(m))
                self._channel[m['chanId']] = m['channel']
        elif type(m) == list:
            if m[1] == 'hb': return # heartbeat
            chan, contents = m[0], m[1:]
            chan = self._channel.get(chan, None)
            if chan == 'trades':
                if contents[0] not in ('te', 'tu'): return # skip snapshot
                if contents[0] != 'tu': return
                #common.logger.info("trading " + repr(contents))
                b = self._getTradeBar(contents)
                self.__queue.put((WebSocketClient.ON_TRADE, b))
            elif chan == 'book':
                self._book.update(toMarketMessage(contents, 'BTCUSD'))
                b = self._book.OrderBookUpdate()
                #common.logger.info("updating " + repr(b))
                self.__queue.put((WebSocketClient.ON_ORDER_BOOK_UPDATE, b))

    def _getTradeBar(self, contents):
        # hackery to make datetime be monotonic
        b = TradeBar.fromBitfinexTrade(contents)
        if b.getDateTime() <= self.__lastTradeTime:
            state = list(b.__getstate__())
            state[0] = self.__lastTradeTime + 0.001
            b.__setstate__(state)
        self.__lastTradeTime = b.getDateTime()
        return b

    def onClosed(self, code, reason):
        common.logger.info("Closed. Code: %s. Reason: %s." % (code, reason))
        self.__queue.put((WebSocketClient.ON_DISCONNECTED, None))

    def onDisconnectionDetected(self):
        common.logger.warning("Disconnection detected.")
        try:
            self.stopClient()
        except Exception, e:
            common.logger.error("Error stopping websocket client: %s." % (str(e)))
        self.__queue.put((WebSocketClient.ON_DISCONNECTED, None))





class WebSocketClientThread(threading.Thread):
    def __init__(self):
        super(WebSocketClientThread, self).__init__()
        self.__wsClient = WebSocketClient()

    def getQueue(self):
        return self.__wsClient.getQueue()

    def start(self):
        self.__wsClient.connect()
        super(WebSocketClientThread, self).start()

    def run(self):
        self.__wsClient.startClient()

    def stop(self):
        try:
            common.logger.info("Stopping websocket client.")
            self.__wsClient.stopClient()
        except Exception, e:
            common.logger.error("Error stopping websocket client: %s." % (str(e)))
