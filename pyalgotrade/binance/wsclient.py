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
import logging
import threading
from datetime import datetime

from .. import bar
from .. import logger as pyalgo_logger
from ..bar import TradeBar
from ..websocket.client import WebSocketClientBase
from ..orderbook import OrderBook, MarketUpdate
from ..websocket.streamsync import StreamSynchronizer
from .netclients import toBookMessages, LOCAL_SYMBOL, BinanceRest


logger = pyalgo_logger.getLogger(__name__)

loglevel = logger.getEffectiveLevel()
logging.getLogger("tornado.access").setLevel(loglevel)
logging.getLogger("tornado.application").setLevel(loglevel)
logging.getLogger("tornado.general").setLevel(loglevel)



def get_current_datetime():
    return datetime.now()

EPOCH = datetime(1970,1,1)


class BinanceMatch(object):

    def __init__(self, json):
        self._j = json

    @property
    def time(self):
        return (self.datetime - EPOCH).total_seconds()

    @property
    def datetime(self):
        return datetime.fromtimestamp(self._j['T']/1000.0)

    @property
    def price(self): return float(self._j['p'])

    @property
    def size(self): return float(self._j['q'])

    def involves(self, oidlist):
        for oid in (self._j['a'], self._j['b']):
            if oid in oidlist: return oid
        return None

    @property
    def seq(self): return int(self._j['E'])

    def TradeBar(self):
        open_ = high = low = close = self.price
        volume = self.size
        adjClose = None
        freq = bar.Frequency.TRADE
        #dir_ = TradeBar.UP if self._j['side'] == 'sell' else TradeBar.DOWN
        dir_ = TradeBar.DOWN if self._j['m'] else TradeBar.UP
        tbar = TradeBar(self.datetime, open_, high, low, close, volume, adjClose, freq, dir_)
        tbar._seq = self.seq
        return tbar





class WebSocketClient(WebSocketClientBase):

    # Events
    ON_CONNECTED = object()
    ON_DISCONNECTED = object()
    ON_TRADE = object()
    ON_ORDER_BOOK_UPDATE = object()
    ON_MATCH = object()
    ON_ORDER_CHANGE = object()

    def __init__(self, symbol, key, secret):
        self.symbol = symbol
        localsym = LOCAL_SYMBOL[symbol].lower()
        self._depth_stream = localsym + "@depth"
        self._trade_stream = localsym + "@trade"
        streams = [ self._depth_stream, self._trade_stream ]
        url = "wss://stream.binance.com:9443/ws/" + '/'.join(streams)
        #headers = [("X-MBX-APIKEY", key)]
        headers = []
        logger.info("Initializing connection to " + url + " with headers: " + repr(headers))
        self.__queue = Queue.Queue()
        self.__RESTClient = BinanceRest(key, secret)
        super(WebSocketClient, self).__init__(url, headers=headers)

    def getQueue(self):
        return self.__queue


    ######################################################################
    # WebSocketClientBase events.

    def onOpened(self):
        logger.info("Connected")
        self.__queue.put((WebSocketClient.ON_CONNECTED, None))
        self._book = OrderBook()

        ts_from_stream = lambda m: min(t.rts for t in m.data)
        #stream_newer_than_ts = lambda ts, m: m.data and ts_from_stream(m) > ts
        def stream_newer_than_ts(ts, m):
            return m.data and ts_from_stream(m) >ts

        self.__syncr = StreamSynchronizer(ts_from_stream,
                                          stream_newer_than_ts,
                                          self._apply_update,
                                          self._apply_full)

        data = self.__RESTClient.book_snapshot()
        self.__syncr.submit_syncdata(data)

    def _apply_update(self, u):
        self._book.update(u)
        b = self._book.marketsnapshot()
        self.__queue.put((WebSocketClient.ON_ORDER_BOOK_UPDATE, b))

    def _apply_full(self, syncdata):
        self._book.update(syncdata)
        logger.info("got sync")
        return syncdata.data[0].rts


    def onMessage(self, m):
        #is_depth = lambda : m['stream'] == self._depth_stream
        #is_trade = lambda : m['stream'] == self._trade_stream
        is_depth = lambda : m['e'] == "depthUpdate"
        is_trade = lambda : m['e'] == "trade"
        if is_depth():
            # orderbook update
            bms = toBookMessages(m, self.symbol)
            u = MarketUpdate(ts=bms[0].rts, data=bms)
            self.__syncr.submit_streamdata(u)
        elif is_trade():
            # trade tick
            cbm = BinanceMatch(m)
            self.__queue.put((WebSocketClient.ON_MATCH, cbm))
            self.__queue.put((WebSocketClient.ON_TRADE, cbm.TradeBar()))
        else:
            logger.error("Unknown Stream type in message: " + repr(m))
            return

    def onClosed(self, code, reason):
        logger.info("Closed. Code: %s. Reason: %s." % (code, reason))
        self.__queue.put((WebSocketClient.ON_DISCONNECTED, None))

    def onDisconnectionDetected(self):
        logger.warning("Disconnection detected.")
        try:
            self.stopClient()
        except Exception as e:
            logger.error("Error stopping websocket client: %s." % (str(e)))
        self.__queue.put((WebSocketClient.ON_DISCONNECTED, None))





class WebSocketClientThread(threading.Thread):
    def __init__(self, *a, **kw):
        super(WebSocketClientThread, self).__init__()
        self.__wsClient = WebSocketClient(*a, **kw)
        self.__wsClient.setKeepAliveMgr(None) # send no keepalives

    def getQueue(self):
        return self.__wsClient.getQueue()

    def start(self):
        logger.info("Connecting websocket client and starting thread.")
        self.__wsClient.connect()
        super(WebSocketClientThread, self).start()
        logger.info("Websocket client thread started.")


    def run(self):
        logger.info("Starting websocket client in client thread.")
        self.__wsClient.startClient() # this is the tornado IOLoop

    def stop(self):
        try:
            logger.info("Stopping websocket client.")
            self.__wsClient.stopClient()
        except Exception as e:
            logger.error("Error stopping websocket client: %s." % (str(e)))
