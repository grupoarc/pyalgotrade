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

import time
import Queue

from pyalgotrade import bar
from pyalgotrade import barfeed
from pyalgotrade import observer
from pyalgotrade.coinbase import common
from pyalgotrade.coinbase import wsclient


class LiveTradeFeed(barfeed.BaseBarFeed):

    """A real-time BarFeed that builds bars from live trades.

    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded
        from the opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        Note that a Bar will be created for every trade, so open, high, low and close values will all be the same.
    """

    QUEUE_TIMEOUT = 0.01

    def __init__(self, maxLen=None):
        super(LiveTradeFeed, self).__init__(bar.Frequency.TRADE, maxLen)
        self.__barDicts = []
        self.registerInstrument(common.btc_symbol)
        self.__prevTradeDateTime = None
        self.__thread = None
        self.__initializationOk = None
        self.__enableReconnection = True
        self.__stopped = False
        self.__orderBookUpdateEvent = observer.Event()
        self.__matchEvent = observer.Event()

    # Factory method for testing purposes.
    def buildWebSocketClientThread(self):
        return wsclient.WebSocketClientThread()

    def getCurrentDateTime(self):
        return wsclient.get_current_datetime()

    def enableReconection(self, enableReconnection):
        self.__enableReconnection = enableReconnection

    def __initializeClient(self):
        self.__initializationOk = None
        common.logger.info("Initializing websocket client.")

        try:
            # Start the thread that runs the client.
            self.__thread = self.buildWebSocketClientThread()
            self.__thread.start()
        except Exception, e:
            self.__initializationOk = False
            common.logger.error("Error connecting : %s" % str(e))

        # Wait for initialization to complete.
        while self.__initializationOk is None and self.__thread.is_alive():
            self.__dispatchImpl([wsclient.WebSocketClient.ON_CONNECTED])

        if self.__initializationOk:
            common.logger.info("Initialization ok.")
        else:
            common.logger.error("Initialization failed.")
        return self.__initializationOk

    def __onDisconnected(self):
        if self.__enableReconnection:
            initialized = False
            while not self.__stopped and not initialized:
                common.logger.info("Reconnecting")
                initialized = self.__initializeClient()
                if not initialized:
                    time.sleep(5)
        else:
            self.__stopped = True

    def __dispatchImpl(self, eventFilter):
        ret = False
        try:
            eventType, eventData = self.__thread.getQueue().get(True, LiveTradeFeed.QUEUE_TIMEOUT)
            if eventFilter is not None and eventType not in eventFilter:
                return False

            ret = True
            if eventType == wsclient.WebSocketClient.ON_TRADE:
                self.__barDicts.append({ common.btc_symbol: eventData })
            elif eventType == wsclient.WebSocketClient.ON_MATCH:
                self.__matchEvent.emit(eventData)
            elif eventType == wsclient.WebSocketClient.ON_ORDER_BOOK_UPDATE:
                self.__orderBookUpdateEvent.emit(eventData)
            elif eventType == wsclient.WebSocketClient.ON_CONNECTED:
                self.__initializationOk = True
            elif eventType == wsclient.WebSocketClient.ON_DISCONNECTED:
                self.__onDisconnected()
            else:
                ret = False
                common.logger.error("Invalid event received to dispatch: %s - %s" % (eventType, eventData))
        except Queue.Empty:
            pass
        return ret

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        ret = None
        if len(self.__barDicts):
            ret = bar.Bars(self.__barDicts.pop(0))
        return ret

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    # This may raise.
    def start(self):
        super(LiveTradeFeed, self).start()
        if self.__thread is not None:
            pass
            #raise Exception("Already running")
        elif not self.__initializeClient():
            self.__stopped = True
            raise Exception("Initialization failed")

    def dispatch(self):
        # Note that we may return True even if we didn't dispatch any Bar
        # event.
        if self.__dispatchImpl(None): return True
        if super(LiveTradeFeed, self).dispatch(): return True
        return False

    # This should not raise.
    def stop(self):
        try:
            self.__stopped = True
            if self.__thread is not None and self.__thread.is_alive():
                common.logger.info("Shutting down websocket client.")
                self.__thread.stop()
        except Exception, e:
            common.logger.error("Error shutting down client: %s" % (str(e)))

    def isAlive(self):
        return self.__thread and self.__thread.isAlive()

    # This should not raise.
    def join(self):
        if self.__thread is not None:
            self.__thread.join()

    def eof(self):
        return self.__stopped

    def getOrderBookUpdateEvent(self):
        """
        Returns the event that will be emitted when the orderbook gets updated.

        Eventh handlers should receive one parameter:
         1. A :class:`pyalgotrade.bitstamp.wsclient.OrderBookUpdate` instance.

        :rtype: :class:`pyalgotrade.observer.Event`.
        """
        return self.__orderBookUpdateEvent

    def getMatchEvent(self):
        return self.__matchEvent


