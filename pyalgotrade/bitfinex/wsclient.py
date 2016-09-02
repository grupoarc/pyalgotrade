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
from pyalgotrade.orderbook import OrderBook

from pyalgotrade.bitfinex.netclients import toMarketMessage


def get_current_datetime():
    return datetime.now()

class TradeBar(bar.BasicBar):

    UP = 'UP'
    DOWN = 'DOWN'

    def __init__(self, time, open_, high, low, close, volume, adjClose, freq, direction):
        super(TradeBar, self).__init__(time, open_, high, low, close, volume, adjClose, freq)
        self.__direction = direction

    def getDirection(self):
        return self.__direction


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
        self._book = OrderBook("bitfinex", "BTCUSD")
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
                b = self._book.marketsnapshot()
                #common.logger.info("updating " + repr(b))
                self.__queue.put((WebSocketClient.ON_ORDER_BOOK_UPDATE, b))

    def _getTradeBar(self, contents):
        ttype = contents[0]
        if ttype == 'te':
            seq, timestamp, price, size = contents[1:]
        elif ttype == 'tu':
            seq, tid, timestamp, price, size = contents[1:]
        else:
            common.logger.error("unknown bitfinex trade type: " + repr(contents))
            raise SyntaxError("unknown bitfinex trade type: " + repr(contents))
        size, price, ts = float(size), float(price), float(timestamp)
        open_ = high = low = close = price
        freq = bar.Frequency.TRADE
        direction = TradeBar.DOWN if size>0 else TradeBar.UP
        # hackery to make datetime be monotonic
        if ts <= self.__lastTradeTime:
            ts = self.__lastTradeTime + 0.001
        self.__lastTradeTime = ts
        return TradeBar(ts, open_, high, low, close, abs(size), close, freq, direction)

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
