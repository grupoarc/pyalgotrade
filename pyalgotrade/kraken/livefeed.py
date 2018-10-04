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

#import time
import datetime

from six.moves import queue

from .. import bar
from .. import barfeed
from .. import observer
from . import common
from .netclients import MultiPoller, KrakenRest



class LiveFeed(barfeed.BaseBarFeed):
    """A streaming feed.
      It will one or both of feed book updates and order changes,
      if they're subscribed to before .start() is called .
      If they're not subscribed to, they'll be omitted for efficiency.
    """

    QUEUE_TIMEOUT = 0.01

    def __init__(self, key, secret, symbol, maxLen=None):
        super(LiveFeed, self).__init__(bar.Frequency.SECOND, maxLen)
        self.__httpClient = KrakenRest(key, secret)
        self.registerInstrument(symbol)
        self.__orderBookUpdateEvent = observer.Event()
        self.__matchEvent = observer.Event()
        self.__changeEvent = observer.Event()
        self.EVENT_HANDLER = {
                              MultiPoller.ON_ORDER_BOOK_UPDATE: self.__orderBookUpdateEvent,
                              MultiPoller.ON_ORDER_UPDATE : self.__changeEvent,
                              }
        self.__poller = None

    def http_client(self):
        return self.__httpClient

    def __dispatch(self, eventFilter):
        try:
            eventType, eventData = self.__poller.get(True, self.QUEUE_TIMEOUT)
        except queue.Empty:
            return False

        if eventFilter is not None and eventType not in eventFilter:
            return False

        todo = self.EVENT_HANDLER.get(eventType)
        if todo is None:
            common.logger.error("Invalid event received to dispatch: %s - %s" % (eventType, eventData))
            return False

        todo.emit(eventData)
        return True

    # Interface below here

    def getCurrentDateTime(self):
        return datetime.now()

    # This may raise.
    def start(self):
        super(LiveFeed, self).start()
        if self.__poller is None:
            to_poll = [ e for e, h in self.EVENT_HANDLER.items() if h.hasSubscribers() ]
            common.logger.info("Polling for " + repr(to_poll))
            symbol = self.getDefaultInstrument()
            self.__poller = MultiPoller(self.__httpClient, symbol, poll_frequency=1, to_poll=to_poll)
        if not self.__poller.is_alive(): self.__poller.start()

    def dispatch(self):
        # Note that we may return True even if we didn't dispatch any Bar
        # event.
        # don't short-circuit this test because we need the side effects
        dispatched = [super(LiveFeed, self).dispatch(),
                      self.__dispatch(None)
                     ]
        return any(dispatched)

    def stop(self):
        if self.__poller is not None:
            self.__poller.stop()

    def join(self):
        if self.__poller is not None:
            self.__poller.join()

    def getOrderBookUpdateEvent(self):
        """
        Returns the event that will be emitted when the orderbook gets updated.

        Eventh handlers should receive one parameter:
         1. A :class:`pyalgotrade.bitstamp.wsclient.OrderBookUpdate` instance.

        :rtype: :class:`pyalgotrade.observer.Event`.
        """
        return self.__orderBookUpdateEvent

    def getMatchEvent(self):
        # this won't emit anything for this impl, but generic strats may still want to subscribe
        return self.__matchEvent

    def getChangeEvent(self):
        # this won't emit anything for this impl, but generic strats may still want to subscribe
        return self.__changeEvent

    def barsHaveAdjClose(self):
        return False

    def eof(self):
        return not self.isAlive()

    def getNextBars(self):
        ret = None
        #if len(self.__barDicts):
        #    ret = bar.Bars(self.__barDicts.pop(0))
        return ret

    def peekDateTime(self):
        # Return None since this is a realtime subject.
        return None

    def isAlive(self):
        return self.__poller and not self.__poller.stopped()

LiveBookFeed = LiveFeed
