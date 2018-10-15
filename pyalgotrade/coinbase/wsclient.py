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

import ujson as json
from datetime import datetime
import threading
import Queue

from pyalgotrade import bar
from pyalgotrade.broker import Order, OrderEvent, OrderExecutionInfo
from pyalgotrade.websocket.client import WebSocketClientBase
from pyalgotrade.coinbase import common
from pyalgotrade.orderbook import OrderBook, MarketUpdate
from pyalgotrade.coinbase.streamsync import StreamSynchronizer

from pyalgotrade.coinbase.netclients import toBookMessages


def get_current_datetime():
    return datetime.now()

EPOCH = datetime(1970,1,1)


class CoinbaseMatch(object):

    def __init__(self, json):
        self._j = json

    @property
    def time(self):
        return (self.datetime - EPOCH).total_seconds()

    @property
    def datetime(self):
        return datetime.strptime(self._j['time'], "%Y-%m-%dT%H:%M:%S.%fZ")

    @property
    def price(self): return float(self._j['price'])

    @property
    def size(self): return float(self._j['size'])

    def involves(self, oidlist):
        for oid in (self._j['maker_order_id'], self._j['taker_order_id']):
            if oid in oidlist: return oid
        return None

    @property
    def seq(self): return int(self._j['sequence'])

    def TradeBar(self):
        open_ = high = low = close = self.price
        volume = self.size
        adjClose = None
        freq = bar.Frequency.TRADE
        dir_ = bar.TradeBar.UP if self._j['side'] == 'sell' else bar.TradeBar.DOWN
        tbar = bar.TradeBar(self.datetime, open_, high, low, close, volume, adjClose, freq, dir_)
        tbar._seq = self.seq
        return tbar



class OrderStateChange(object):

    def __init__(self, json):
        self._j = json
        self._id = None
        self._price = None
        self._new_state, self._event_type = None, None

    @property
    def id(self):
        if self._id is None: self.__parse()
        return self._id

    @property
    def new_state(self):
        if self._id is None: self.__parse()
        return self._new_state

    @property
    def event_type(self):
        if self._id is None: self.__parse()
        return self._event_type

    def __parse(self):
        json = self._j
        self._id = json['order_id']
        mtype = json['type']
        s, e = None, None
        if mtype == 'received':
            s, e =  Order.State.ACCEPTED, OrderEvent.Type.ACCEPTED
        elif mtype == 'done':
            if json['reason'] == 'filled' and json.get('remaining_size', None) == '0':
                s, e = Order.State.FILLED, OrderEvent.Type.FILLED
            else:
                s, e = Order.State.CANCELED, OrderEvent.Type.CANCELED
        price = json.get('price', None)
        self._price = float(price) if price is not None else None
        self._new_state, self._event_type = s, e

    def oei(self, order):
        if self._j.get('reason','') != 'filled': return None
        dt = datetime.strptime(self._j['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
        ounfilled = order.getQuantity() - order.getFilled()
        sizeleft = float(self._j.get('remaining_size', 0.0))
        if self._price is None: self.__parse()
        return OrderExecutionInfo(self._price, ounfilled - sizeleft, 0, dt)



class WebSocketClient(WebSocketClientBase):

    # Events
    ON_CONNECTED = object()
    ON_DISCONNECTED = object()
    ON_TRADE = object()
    ON_ORDER_BOOK_UPDATE = object()
    ON_MATCH = object()
    ON_ORDER_CHANGE = object()

    def __init__(self, symbol):
        url = "wss://ws-feed.gdax.com"
        super(WebSocketClient, self).__init__(url)
        self.__queue = Queue.Queue()
        self.__symbol = symbol

    def getQueue(self):
        return self.__queue


    ######################################################################
    # WebSocketClientBase events.

    def onOpened(self):
        self.__queue.put((WebSocketClient.ON_CONNECTED, None))
        common.logger.info("Connected; subscribing.")
        subscribe = json.dumps({ "type": "subscribe", "product_id": "BTC-USD" })
        self.send(subscribe)
        self._book = OrderBook()

        ts_from_stream = lambda m: min(t.rts for t in m.data)
        stream_newer_than_ts = lambda ts, m: m.data and ts_from_stream(m) > ts

        self.__syncr = StreamSynchronizer(ts_from_stream,
                                          stream_newer_than_ts,
                                          self._apply_update,
                                          self._apply_full)

        from pyalgotrade.coinbase.netclients import CoinbaseRest
        data = CoinbaseRest(None, None, None).book_snapshot()
        self.__syncr.submit_syncdata(data)
        #common.logger.info("done opening")

    def _apply_update(self, u):
        self._book.update(u)
        b = self._book.marketsnapshot()
        self.__queue.put((WebSocketClient.ON_ORDER_BOOK_UPDATE, b))

    def _apply_full(self, syncdata):
        self._book.update(syncdata)
        #common.logger.info("got sync")
        return syncdata.data[0].rts


    def onMessage(self, m):
        mtype = m['type']
        if mtype == 'heartbeat': return
        if mtype == 'error':
            common.logger.error("coinbase ws error: " + repr(m))
            return
        if not mtype in ('received', 'open', 'done', 'match', 'change'):
            common.logger.warning("Unknown coinbase websocket msg: " + repr(m))
            return
        if mtype == 'match':
            cbm = CoinbaseMatch(m)
            self.__queue.put((WebSocketClient.ON_MATCH, cbm))
            self.__queue.put((WebSocketClient.ON_TRADE, cbm.TradeBar()))
        elif mtype in ('received', 'done'):
            self.__queue.put((WebSocketClient.ON_ORDER_CHANGE, OrderStateChange(m)))
        bms = toBookMessages(m, self.__symbol)
        if bms:
            u = MarketUpdate(ts=get_current_datetime(), data=bms)
            self.__syncr.submit_streamdata(u)

    def onClosed(self, code, reason):
        common.logger.info("Closed. Code: %s. Reason: %s." % (code, reason))
        self.__queue.put((WebSocketClient.ON_DISCONNECTED, None))

    def onDisconnectionDetected(self):
        common.logger.warning("Disconnection detected.")
        try:
            self.stopClient()
        except Exception as e:
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
        except Exception as e:
            common.logger.error("Error stopping websocket client: %s." % (str(e)))
