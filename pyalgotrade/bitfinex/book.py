0# ---------------------------------------------------------------------------
# From-scratch book implementation;  errs on side of simplicity.
# ---------------------------------------------------------------------------

import time
from collections import deque, namedtuple
from attrdict import AttrDict
from sortedcontainers import SortedDict

# ---------------------------------------------------------------------------
# Nb., unlike previous impl., this is a generic book updated generically
# ---------------------------------------------------------------------------

Bid = "bid"
Ask = "ask"

AbstractMarketDataWrapper = namedtuple('AbstractMarketDataWrapper', 'ts venue symbol data')
AbstractMarketDataWrapper.__new__.__defaults__ = (0, '', '', [])

class MarketUpdate(AbstractMarketDataWrapper): pass
class MarketSnapshot(AbstractMarketDataWrapper): pass # unenforced, but data should only contain Assigns

AbstractMarketDataDelta = namedtuple('AbstractMarketDataDelta', 'rts venue symbol price size side')
AbstractMarketDataDelta.__new__.__defaults__ = (0, '', '', 0, 0, Ask)

class Assign(AbstractMarketDataDelta): pass
class Increase(AbstractMarketDataDelta): pass
class Decrease(AbstractMarketDataDelta): pass
class Trade(AbstractMarketDataDelta): pass

PriceLevel = Assign

class OrderBookUpdate(object):
    """An order book update event."""

    def __init__(self, dateTime, askPrices, askVolumes, bidPrices, bidVolumes):
        self.__dateTime = dateTime
        self.__askPrices = askPrices
        self.__askVolumes = askVolumes
        self.__bidPrices = bidPrices
        self.__bidVolumes = bidVolumes

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` when this event was received."""
        return self.__dateTime

    def getBidPrices(self):
        """Returns a list with the top 20 bid prices."""
        return self.__bidPrices

    def getBidVolumes(self):
        """Returns a list with the top 20 bid volumes."""
        return self.__bidVolumes

    def getAskPrices(self):
        """Returns a list with the top 20 ask prices."""
        return self.__askPrices

    def getAskVolumes(self):
        """Returns a list with the top 20 ask volumes."""
        return self.__askVolumes

    def __repr__(self):
        return "<OrderBookUpdate %r bid: %r %r ask: %r %r >" % (
            self.__dateTime,
            self.__bidPrices, self.__bidVolumes,
            self.__askPrices, self.__askVolumes)



class Book():
    """
    Generic book; understands only common messages (for updating the book)
    Note: prices and sizes are Decimals (already decoded). Implements L1/L2.
    """
    def __init__(self, venue=None, symbol=None):
        self.venue   = venue
        self.symbol  = symbol
        self.trades = deque(maxlen=100)  # Index [0] is most recent trade
        self.reset()

    def reset(self):
        self.bids   = SortedDict(lambda k:-k, {})   # maps price: PriceLevel(size, tick)
        self.asks   = SortedDict({})   # maps price: PriceLevel(size, tick)
        self.last   = None # the last MarketUpdate or MarketSnapshot

    def is_empty(self):
        return self.last is None
        #return not (self.bids and self.asks)

    def update(self, update):

        def set_pricelevel(side, assign):
            ap = assign.price
            if assign.size > 0: side[ap] = assign
            elif ap in side: del side[ap]
        s_pl = set_pricelevel

        def make_assign(update, **kwargs):
            assign = update._asdict()
            assign.update(kwargs)
            return PriceLevel(**assign)
        mk_a = make_assign

        g_sz = lambda s, p: s.get(p, PriceLevel()).size

        # check type(update) == MarketUpdate ?
        if type(update) == MarketSnapshot: self.reset()
        for t in update.data:
            tt = type(t)
            if tt == Trade:
                self.trades += t
                continue
            s = { Ask: self.asks, Bid: self.bids }.get(t.side, None)
            if s is None: raise ValueError("Unknown side: %r" % t.side)
            tp, ts = t.price, t.size
            if   tt == Assign:   s_pl(s, t)
            elif tt == Increase: s_pl(s, mk_a(t, size=g_sz(s, tp) + ts))
            elif tt == Decrease: s_pl(s, mk_a(t, size=g_sz(s, tp) - ts))
            else: raise ValueError("Unknown type %r of %r" % (type(t), t))

        self.last = update
        return self


    def get_marketsnapshot(self):
        data = self.bids.values() + self.asks.values()
        return MarketSnapshot(time.time(), self.venue, self.symbol, data)

    @classmethod
    def from_snapshot(cls, snapshot):
        return cls(snapshot.venue, snapshot.symbol).update(snapshot)

    @property
    def inside(self):
        # problem: self.bids.keys() is [], because book is empty
        bid_price, ask_price = self.bids.iloc[0], self.asks.iloc[0]
        return AttrDict({
            'bid': self.bids[bid_price],
            'ask': self.asks[ask_price]
        })

    @property
    def inside_bid(self):
        try:
            return self.bids[self.bids.iloc[0]]
        except IndexError:
            print("!!! Book for venue %s:%s bids are empty!!"%(self.venue, self.symbol))
            raise

    @property
    def inside_ask(self):
        try:
            return self.asks[self.asks.iloc[0]]
        except IndexError:
            print("!!! Book for venue %s:%s asks are empty!!"%(self.venue, self.symbol))
            raise

    def nvolume(self, nlevels=None):
        """ return the inside <nlevels> levels on each side of the book
              nlevels = None (the default) means 'all'
        """
        bids = self.bids.values()[:nlevels]
        asks = self.asks.values()[:nlevels]
        return { 'bids': list(bids), 'asks': list(asks) }

    def price_for_size(self, side, size):
        """
        The cost of the specifed size on the specified side.
        Note that this is not 'to fill an order on the specified side',
        because Asks fill Bid orders and vice versa.
        """
        pside = { Bid: self.bids, Ask: self.asks }[side]
        sizeleft = size
        value = 0
        for price in pside:
            pl = pside[price]
            s = min(sizeleft, pl.size)
            value += s * price
            sizeleft -= s
            if not sizeleft: break
        return value

    def npfs(self, size):
        return self.price_for_size(Bid, size)/self.price_for_size(Ask, size)

    def size_for_price(self, side, price):
        """
        How much size the specified price is worth on the specified side.
        """
        pside = { Bid: self.bids, Ask: self.asks }[side]
        priceleft = price
        size = 0
        for price in pside:
            pl = pside[price]
            p = price * pl.size
            if p > priceleft:
                priceleft -= p
                size += pl.size
            else:
                size += priceleft / price
                break
        return size

    def OrderBookUpdate(self):
        return OrderBookUpdate(self.last.ts,
                               [ self.asks[k].price for k in self.asks.iloc[0:20] ],
                               [ self.asks[k].size  for k in self.asks.iloc[0:20] ],
                               [ self.bids[k].price for k in self.bids.iloc[0:20] ],
                               [ self.bids[k].size  for k in self.bids.iloc[0:20] ],
                                )

