
from __future__ import print_function

from datetime import datetime
import hmac, hashlib, time, requests, base64, threading, Queue
#import ujson as json

from requests.auth import AuthBase

from .. import broker
from .. import logger as pyalgo_logger
from ..broker import FloatTraits, OrderExecutionInfo
from ..orderbook import Ask, Bid, Assign, MarketSnapshot

from . import VENUE, LOCAL_SYMBOL, SYMBOL_LOCAL, SYMBOLS

logger = pyalgo_logger.getLogger("kraken")



def flmath(n):
    return round(n, 12)

def fees(txnsize):
    return flmath(txnsize * float('0.0025'))



# ---------------------------------------------------------------------------
# Turn a kraken order into a pyalgotrade Order
# ---------------------------------------------------------------------------

from attrdict import AttrDict

KrakenOrder = AttrDict

#KrakenOrder = namedtuple('KrakenOrder', 'pair side type price price2 volume leverage oflags starttm expiretm userref close_type close_price close_price2')
#KrakenOrder.__new__.__defaults__ = (None,) * len(KrakenOrder._fields)



# ---------------------------------------------------------------------------
#  Utilities
# ---------------------------------------------------------------------------

from ..utils import memoize as lazy_init


# ---------------------------------------------------------------------------
#  Kraken authentication helper
# ---------------------------------------------------------------------------

class KrakenAuth(AuthBase):
    """a requests-module-compatible auth module"""
    def __init__(self, key, secret):
        self.api_key    = key
        self.secret_key = secret

    def __call__(self, request):
        nonce = int(1000*time.time())
        request.data = getattr(request, 'data', {})
        request.data['nonce'] = nonce
        request.prepare_body(request.data, []) # build request.body from request.data

        message = request.path_url + hashlib.sha256(str(nonce) + request.body).digest()
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha512).digest()

        request.headers.update({
            'API-Key': self.api_key,
            'API-Sign': base64.b64encode(signature)
        })
        return request


# ---------------------------------------------------------------------------
#  Kraken REST client
# ---------------------------------------------------------------------------

URL = "https://api.kraken.com/0/"

class KrakenRest(object):

    #GTC = GOOD_TIL_CANCEL = object()
    #GTT = GOOD_TIL_TIME = object()
    #IOC = IMMEDIATE_OR_CANCEL = object()
    #FOK = FILL_OR_KILL = object()
    POST_ONLY = object()

    def __init__(self, key, secret):
        self.__auth = KrakenAuth(key, secret)

    def auth(self): return self.__auth

    @property
    @lazy_init
    def _session(self):
        return requests.Session()

    def _request(self, method, url, **kwargs):
        raise_errors = kwargs.get('raise_errors', True)
        if 'raise_errors' in kwargs: del kwargs['raise_errors']
        result = self._session.request(method, URL + url, **kwargs)
        if raise_errors:
            try:
                result.raise_for_status() # raise if not status == 200
                if result.json().get('error'):
                    raise ValueError()
            except Exception:
                print("ERROR: " + method + " " + url + " " + repr(kwargs) + " GOT: " + result.text)
                raise
        return result

    def _auth_request(self, method, url, **kwargs):
        if not 'auth' in kwargs: kwargs['auth'] = self.auth()
        return self._request(method, url, **kwargs)

    def _get(self, url, **kwargs): return self._request('GET', url, **kwargs)
    def _getj(self, url, **kwargs): return self._get('public/' + url, **kwargs).json()
    def _auth_getj(self, url, **kwargs): return self._auth_request('GET', 'private/' + url, **kwargs).json()
    def _auth_postj(self, url, **kwargs): return self._auth_request('POST', 'private/' + url, **kwargs).json()

    #
    # Market data (public endpoints)
    #

    def server_time(self):
        return self._getj('Time')

    def assets(self):
        return self._getj('Assets')

    def asset_pairs(self):
        return self._getj('AssetPairs')['result']

    def ticker(self):
        return self._getj('Ticker')

    def OHLC(self):
        return self._getj('OHLC')

    def book(self, pair, count=None):
        params = { 'pair': pair }
        if count is not None: params['count'] = count
        return self._getj('Depth', params=params)

    def trades(self, pair, since=None):
        params = { 'pair': pair }
        if since is not None: params['since'] = since
        return self._getj('Trades', params=params)

    def spread(self, pair, since=None):
        params = { 'pair': pair }
        if since is not None: params['since'] = since
        return self._getj('Spread', params=params)

    #
    # Private endpoints
    #

    def accounts(self):
        return self._auth_postj('Balance')

    def trade_balance(self, aclass=None, asset=None):
        params = {}
        if aclass is not None: params['aclass'] = aclass
        if asset is not None: params['asset'] = asset
        return self._auth_postj('TradeBalance', data=params)

    def open_orders(self, trades=False, userref=None):
        params = { 'trades': 'true' if trades else 'false' }
        if userref is not None: params['userref'] = userref
        #return self._auth_postj('OpenOrders', data=params)['result']['open']
        res = self._auth_postj('OpenOrders', data=params)
        return res['result']['open']

    def closed_orders(self, offset, trades=False, userref=None, start=None, end=None, closetime=None):
        """
        offset - result offset
        closetime is one of 'open', 'close' or 'both'
        """
        params = { 'ofs': offset, trades: 'true' if trades else 'false', closetime: closetime or 'both' }
        if userref is not None: params['userref'] = userref
        if start is not None: params['start'] = start
        if end is not None: params['end'] = end
        return self._auth_postj('ClosedOrders', data=params)

    def query_orders(self, *txids, **kwargs):
        params = { 'txid': ','.join(str(i) for i in txids) }
        params['trades'] = bool(kwargs.get('trades'))
        if kwargs.get('userref') is not None:
            params['userref'] = kwargs['userref']
        return self._auth_postj('QueryOrders', data=params)

    def trades_history(self, ofs, ttype='all', trades=False, start=None, end=None):
        # ttype is one of 'all', 'any position', 'closed position', 'closing position', 'no position'
        params = { 'ofs': ofs, 'type': ttype }
        if start is not None: params['start'] = start
        if end is not None: params['end'] = end
        if trades: params['trades'] = 'true'
        return self._auth_postj('TradesHistory', data=params)

    def query_trades(self, *txids, **kwargs):
        params = { 'txid': ','.join(str(i) for i in txids) }
        params['trades'] = bool(kwargs.get('trades'))
        return self._auth_postj('QueryTrades', data=params)

    def open_positions(self, *txids, **kwargs):
        params = { 'txid': ','.join(str(i) for i in txids) }
        params['docalcs'] = bool(kwargs.get('docalcs'))
        return self._auth_postj('OpenPositions', data=params)

    def ledgers(self, ofs, aclass='currency', asset=None, ltype='all', start=None, end=None):
        params = { 'ofs': ofs, 'aclass': aclass, 'type': ltype }
        if asset is not None:
            params['asset'] = asset if type(asset) in (type(''), type(u'')) else ','.join(asset)
        if start is not None: params['start'] = start
        if end is not None: params['end'] = end
        return self._auth_postj('Ledgers', data=params)

    def query_ledgers(self, *ids):
        todo = ids
        result = {}
        while todo:
            ids = ','.join(todo[:20])
            result.update(self._auth_postj('QueryLedgers', data={'id': ids}))
            todo = todo[20:]
        return result

    def trade_volume(self, pairs=None, fee_info=None):
        params = {}
        if pairs is not None:
            params['pair'] = pairs if type(pairs) in (type(''), type(u'')) else ','.join(pairs)
        if fee_info is not None:
            params['fee-info'] = 'true' if fee_info else 'false'
        return self._auth_postj('TradeVolume', data=params)


    def place_order(self, pair, side, otype, size, oflags=None, **kwargs):
        params = {
            'pair': pair,
            'type': { Bid: "buy", Ask: "sell" }[side],
            'ordertype': otype,
            'volume': size
        }
        LOCAL_FLAGS = { self.POST_ONLY: 'post' }
        for k in ('price', 'price2', 'leverage', 'starttm', 'expiretm', 'userref', 'close', 'validate'):
            if k in kwargs: params[k] = kwargs[k]
        if oflags is not None: params['oflags'] = ','.join(LOCAL_FLAGS[f] for f in oflags)
        logger.debug("AddOrder {!r}".format(params))
        return self._auth_postj('AddOrder', data=params)

    def cancel(self, txid):
        params = { 'txid': txid }
        return self._auth_postj('CancelOrder', data=params)


# impedence match to the rest of the system

    def balances(self):
        return { SYMBOL_LOCAL[k] : v for k, v in self.accounts()['result'].items() }

    def limitorder(self, side, price, size, symbol, flags=()):
        # newOrderId = self.__httpClient.limitorder(side, price, size, flags=flags)
        result = self.place_order(LOCAL_SYMBOL[symbol], side, 'limit', size, price=price, oflags=flags, validate="true")
        if result['error']:
            raise Exception(str(result['error']))
        return result['result']['txid'][0]

    def marketorder(self, side, size, symbol):
        #newOrderId = self.__httpClient.marketorder(side, size)
        result = self.place_order(LOCAL_SYMBOL[symbol], side, 'market', size)
        if result['error']:
            raise Exception(str(result['error']))
        return result['result']['txid'][0]

    def book_snapshot(self, symbol):
        ksymbol = LOCAL_SYMBOL[symbol]
        book = self.book(ksymbol)['result']

        def mkassign(ts, price, size, side):
            return Assign(ts, VENUE, symbol, price, size, side)

        return MarketSnapshot(time.time(), VENUE, symbol,
            [ mkassign(rts, float(price), float(size), Bid) for price, size, rts in book[ksymbol]['bids'] ] +
            [ mkassign(rts, float(price), float(size), Ask) for price, size, rts in book[ksymbol]['asks'] ]
        )

    def inside_bid_ask(self, symbol):
        ksymbol = LOCAL_SYMBOL[symbol]
        book = self.book(ksymbol, 1)['result'][ksymbol]
        bid = book['bids'][0][0]
        ask = book['asks'][0][0]
        #log.info("Got inside bid: {} ask: {}".format(bid, ask))
        return bid, ask

    @lazy_init # cache this, as it doesn't change in the timeframes we care about
    def instrumentTraits(self):
        return { SYMBOL_LOCAL[p]: FloatTraits(d['lot_decimals'], d['pair_decimals']) for p, d in self.asset_pairs().items() }

    def OpenOrders(self):
        return [self._order_to_Order(oinfo, txid) for txid, oinfo in self.open_orders(trades=True).items()]

    def _order_to_Order(self, korder, txid):
        action = { 'buy': broker.Order.Action.BUY,
                  'sell': broker.Order.Action.SELL
                 }.get(korder.descr.type)
        if action is None:
            raise Exception("Invalid order side")
        size = float(korder.vol)
        symbol =  SYMBOL_LOCAL[korder.descr.pair]
        instrumentTraits = self.instrumentTraits()[symbol]

        if korder.descr.ordertype == 'limit': # limit order
            price = float(korder.descr.price)
            o = broker.LimitOrder(action, symbol, price, size, instrumentTraits)
        elif korder.descr.ordertype == 'market':  # Market order
            onClose = False # TBD
            o = broker.MarketOrder(action, symbol, size, onClose, instrumentTraits)
        else:
            raise ValueError("Unsuported Ordertype: " + korder.descr.ordertype)

        if korder.get('opentm'):
            o.setSubmitted(korder.refid, korder.created_at)

        if korder.status == 'pending':
            pass
        elif korder.status == 'open':
            if korder.vol_exc > 0:
                o.setState(broker.Order.State.PARTIALLY_FILLED)
            else:
                o.setState(broker.Order.State.ACCEPTED)
        elif korder.status == 'closed':
            o.setState(broker.Order.State.FILLED)
        elif korder.status == 'canceled':
            o.setState(broker.Order.State.CANCELED)
        elif korder.status == 'expired':
            o.setState(broker.Order.State.CANCELED)

        tradetime2dt = lambda t: datetime.fromtimestamp(t/1000)

        for ttid, tinfo in korder.trades.items():
            o.addExectionInfo(OrderExecutionInfo(tinfo.price, tinfo.vol, tinfo.fee, tradetime2dt(tinfo.time)))

        return o



class FuncPoller(threading.Thread):


    def __init__(self, poll_frequency=1):
        super(FuncPoller, self).__init__()
        self.poll_frequency = poll_frequency
        self.__queue = Queue.Queue()
        self.__running = True

    def _poll(self):
       raise NotImplementedError

    def getQueue(self):
        return self.__queue

    def get(self, *a, **kw):
        return self.__queue.get(*a, **kw)

    def run(self):
        while self.__running:
            try:
                events = list(self._poll())
                if events:
                    logger.info("%d new event(s) found" % (len(events)))
                for e in events:
                    self.__queue.put(e)
            except Exception as e:
                logger.critical("Error retrieving user transactions", exc_info=e)

            # TODO: don't wait full poll_frequency between polls, as that's end-to-start time, and rate-limiting is based on start-to-start time
            time.sleep(self.poll_frequency)

    def stop(self):
        self.__running = False

    def stopped(self):
        return self.__running == False

    def is_alive(self):
        return self.__running and super(FuncPoller, self).is_alive()

    def join(self):
        if self.is_alive():
            super(FuncPoller, self).join()



class BookPoller(FuncPoller):
    """Poller for book updates"""

    ON_ORDER_BOOK_UPDATE = object()

    def __init__(self, httpClient, symbol, poll_frequency):
        self._poll = lambda s: [(self.ON_ORDER_BOOK_UPDATE, httpClient.book_snapshot(symbol))]


class OrderStatusPoller(FuncPoller):
    """Poller for our order status"""

    ON_ORDER_UPDATE = object()

    def __init__(self, httpClient, symbol, poll_frequency):
        self.__client = httpClient

    def __poll(self):
        return [ (self.ORDER_UPDATE, o) for o in self.__client.OpenOrders() ]


class MultiPoller(FuncPoller):
    """Poller for our multiple things - warning, polls any one thing at 1/(poll_frequency*len(to_poll))"""

    ON_ORDER_BOOK_UPDATE = object()
    ON_ORDER_UPDATE = object()

    ACTION = { }

    def __init__(self, httpClient, symbol, poll_frequency, to_poll=()):
        super(MultiPoller, self).__init__(poll_frequency)

        if not symbol in LOCAL_SYMBOL:
            validsyms = ' '.join([str(s) for s in SYMBOLS if len(str(s))>4])
            raise ValueError("Unsupported symbol: {} . Try one of: {}".format(symbol, validsyms))
        self.ACTION = {self.ON_ORDER_BOOK_UPDATE : self.__poll_orderbook,
                       self.ON_ORDER_UPDATE : self.__poll_orders
                       }
        self.__actions = to_poll
        self.__symbol = symbol
        self.__httpClient = httpClient
        self.__action_idx = 0

    def _poll(self):
        if not self.__actions:
            self.__actions = self.ACTION.keys()
        i = self.__action_idx
        self.__action_idx = (i + 1) % len(self.__actions)
        return self.ACTION[self.__actions[i]]()

    def __poll_orderbook(self):
        logger.info("polling orderbook")
        return [(self.ON_ORDER_BOOK_UPDATE, self.__httpClient.book_snapshot(self.__symbol))]

    def __poll_orders(self):
        logger.info("polling open orders")
        return [ (self.ON_ORDER_UPDATE, o) for o in self.__httpClient.OpenOrders() ]

    def feeds(self):
        return self.__actions

    def add_feed(self, action):
        if not action in self.__actions:
            self.__action.append(action)


