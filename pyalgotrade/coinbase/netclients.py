
from __future__ import print_function

import json, hmac, hashlib, time, requests, base64
from datetime import datetime
from decimal import Decimal
from collections import namedtuple

from requests.auth import AuthBase
from .book import Increase, Decrease, Ask, Bid, MarketUpdate, Assign, MarketSnapshot

BTCUSD, BTCEUR = 'BTCUSD', 'BTCEUR'

LOCAL_SYMBOL = { BTCUSD: 'BTC-USD', BTCEUR: 'BTC-EUR' }
SYMBOL_LOCAL = { v: k for k, v in LOCAL_SYMBOL.items() }
SYMBOLS = list(LOCAL_SYMBOL.keys())
VENUE = 'coinbase'


def fees(txnsize):
    return txnsize * Decimal('0.0025')

# ---------------------------------------------------------------------------
#  Coinbase market data message helper / decoder
# ---------------------------------------------------------------------------


def toBookMessages(coinbase_json, symbol):
    """convert a coinbase json message into a list of book messages"""
    cbase = coinbase_json
    if type(cbase) != type({}):
        cbase = json.loads(cbase)
    result = []
    cbt = cbase['type']
    side = { 'buy': Bid, 'sell': Ask }.get(cbase['side'], None)
    if side is None: raise ValueError("Unknown side %r" % cbase['side'])
    if cbt == 'received':
        return []
    if cbt == 'done' and cbase['order_type'] == 'market':
        return []
    time, price = cbase['time'], cbase['price']
    if cbt == 'done':
        mtype, size = Decrease, cbase['remaining_size']
    elif cbt == 'open':
        mtype, size = Increase, cbase['remaining_size']
    elif cbt == 'match':
        mtype, size = Decrease, cbase['size']
    elif cbt == 'change':
        if price == 'null': return []
        mtype = Decrease
        size = Decimal(cbase['old_size']) - Decimal(cbase['new_size'])
    else:
        raise ValueError("Unknown coinbase message: %r" % cbase)
    #rts = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ")
    rts = int(cbase['sequence'])
    result.append(mtype(rts, VENUE, symbol, Decimal(price), Decimal(size), side))
    return result



# ---------------------------------------------------------------------------
#  Coinbase MDC actor - top-level between rest / websocket and hub
# ---------------------------------------------------------------------------

#def CoinbaseMDC(symbol):
#    assert symbol in SYMBOLS # Coinbase only supports these symbols
#    get_syncdata = lambda : CoinbaseRest().book_snapshot(symbol)
#    return DovetailingMDC(VENUE, symbol, get_syncdata, lambda s: CoinbaseWebsocket(s, symbol))


# ---------------------------------------------------------------------------
#  Utilities
# ---------------------------------------------------------------------------


class lazy_init(object):
    """
    A decorator for single, lazy, initialization of (usually) a property
    Could also be viewed as caching the first return value
    """
    def __init__(self, f):
        self.val = None
        self.f = f

    def __call__(self, *args, **kwargs):
        if self.val is None:
            self.val = self.f(*args, **kwargs)
        return self.val


class RateLimiter:
    """
    a function/method call rate limiter.  Calls to .limit() will block
    """
    def __init__(self, calls, secs):
        """Configure the RateLimiter to only allow _calls_ per _secs_"""
        self.calls = calls
        self.secs = secs
        self.lastcalltime = []

    def limit(self):
        maybe = self.lastcalltime + [ time.time() ]
        while maybe[-1] - maybe[0] > self.secs: del maybe[0]
        if len(maybe) <= self.calls:
            self.lastcalltime = maybe
            return

        time.sleep(self.secs - maybe[-1] + maybe[0])
        self.lastcalltime.append(time.time())
        return


# ---------------------------------------------------------------------------
#  Coinbase authentication helper
# ---------------------------------------------------------------------------

class CoinbaseAuth(AuthBase):
    """a requests-module-compatible auth module"""
    def __init__(self, key, secret, passphrase):
        self.api_key    = key
        self.secret_key = secret
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or '')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = signature.digest().encode('base64').rstrip('\n')
        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
        })
        return request


# ---------------------------------------------------------------------------
#  Coinbase REST client
# ---------------------------------------------------------------------------

URL = "https://api.exchange.coinbase.com/"


CoinbaseOrder = namedtuple('CoinbaseOrder', 'id size price done_reason status settled filled_size executed_value product_id fill_fees side created_at done_at')
CoinbaseOrder.__new__.__defaults__ = (None,) * len(CoinbaseOrder._fields)

class CoinbaseRest(object):

    def __init__(self, key, secret, passphrase):
        self.__auth = CoinbaseAuth(key, secret, passphrase)

    def auth(self): return CoinbaseAuth()

    @property
    @lazy_init
    def _session(self):
        return requests.Session()

    ratelimiter = RateLimiter(5, 1)

    def _request(self, method, url, **kwargs):
        self.ratelimiter.limit()
        result = self._session.request(method, URL + url, **kwargs)
        result.raise_for_status() # raise if not status == 200
        return result

    def _auth_request(self, method, url, **kwargs):
        if not 'auth' in kwargs: kwargs['auth'] = self.auth()
        return self._request(method, url, **kwargs)

    def _get(self, url, **kwargs): return self._request('get', url, **kwargs)
    def _getj(self, url, **kwargs): return self._get(url, **kwargs).json()
    def _auth_getj(self, url, **kwargs): return self._auth_request('get', url, **kwargs).json()
    def _auth_postj(self, url, **kwargs): return self._auth_request('post', url, **kwargs).json()
    def _auth_delj(self, url, **kwargs): return self._auth_request('delete', url, **kwargs).json()

    #
    # Market data (public endpoints)
    #

    def products(self):
        return self._getj('products')

    def stats(self, symbol=BTCUSD):
        product = LOCAL_SYMBOL[symbol]
        return self._getj("products/" + product + "/stats")

    def server_time(self):
        return self._getj('time')

    #
    # Account (private endpoints)
    #

    def accounts(self, accountId = ""):
        return self._auth_getj( 'accounts/' + accountId)

    def balances(self):
        return { j['currency']: float(j['balance']) for j in self.accounts() }

    #
    # Orders (private endpoints)
    #

    def orders(self, status='all'):
        return self._auth_getj('orders', params={'status': status})

    def Orders(self, status='all'):
        return  [ CoinbaseOrder(**o) for o in self.orders(status) ]

    def order_ids(self, status='all'):
        return [ o['id'] for o in self.orders(status) ]

    def order_statuses(self):
        return [ o['status'] for o in self.orders() ]

    def order(self, id):
        """
        {
        "id": "d50ec984-77a8-460a-b958-66f114b0de9b",
        "size": "3.0",
        "price": "100.23",
        "done_reason": "canceled",
        "status": "done",
        "settled": true,
        "filled_size": "1.3",
        "executed_value": "3.69",
        "product_id": "BTC-USD",
        "fill_fees": "0.001",
        "side": "buy",
        "created_at": "2014-11-14T06:39:55.189376Z",
        "done_at": "2014-11-14T06:39:57.605998Z"
    	}
        """
        return self._auth_getj('orders', id)

    def Order(self, id):
        return CoinbaseOrder(**(self.order(id)))

    def fills(self, order_id):
        params = { 'order_id': order_id }
        return self._auth_getj('fills', params=params)

    def placeOrder(self, order):
        params = {
            'type' : order.order_type,
            'side' : order.side,
            'product_id' : order.product,
            'stp' : order.stp,
            'price' : order.price,
            'size' : order.size,
            'time_in_force' : order.time_in_force,
            'cancel_after' : order.cancel_after
            }
        return self._auth_postj('orders', params=params)

    def limitorder(self, side, price, size, symbol=BTCUSD):
        """Place a limit order"""
        params = {
            'type' : 'limit',
            'side' : side,
            'product_id' : LOCAL_SYMBOL[symbol],
            'price' : price,
            'size' : size
            }
        return self._auth_postj('orders', params=params)['id']

    def marketorder(self, side, size, symbol=BTCUSD):
        """Place a market order"""
        params = {
            'type' : 'market',
            'side' : side,
            'product_id' : LOCAL_SYMBOL[symbol],
            'size' : size
            }
        return self._auth_postj('orders', params=params)['id']

    def cancel(self, orderId=None):
        url = 'orders'
        if orderId is not None: url += '/' + orderId
        return self._auth_delj(url)

    def book(self, symbol=BTCUSD, level=2, raw=False):
        """
        The book looks like ( from https://docs.exchange.coinbase.com/?python#get-product-order-book ):

        {
            "sequence": "3",
            "bids": [
                [ price, size, num-orders ],
                [ "295.96", "4.39088265", 2 ],
                ...
            ],
            "asks": [
                [ price, size, num-orders ],
                [ "295.97", "25.23542881", 12 ],
                ...
            ]
        }
        """
        product = LOCAL_SYMBOL[symbol]
        book = self._get("products/" + product + "/book", params={'level':level})
        if raw: return book.text
        else: return book.json()

    def book_snapshot(self, symbol=BTCUSD):
        book = self.book(symbol)
        def mkassign(ts, price, size, side):
            return Assign(ts, VENUE, symbol, price, size, side)
        rts = book['sequence']
        price = lambda e: Decimal(e[0])
        size = lambda e: Decimal(e[1])
        return MarketSnapshot(time.time(), VENUE, symbol,
            [ mkassign(rts, price(e), size(e), Bid) for e in book['bids'] ] +
            [ mkassign(rts, price(e), size(e), Ask) for e in book['asks'] ]
        )

    def inside_bid_ask(self):
        book = self.book(level=1)
        bid = book['bids'][0][0]
        ask = book['asks'][0][0]
        #log.info("Got inside bid: {} ask: {}".format(bid, ask))
        return bid, ask



#CoinbaseOEC = RESTfulOEC(VENUE,
#                        CoinbaseRest().limitorder,
#                        CoinbaseRest().marketorder,
#                        CoinbaseRest().cancel)
#

