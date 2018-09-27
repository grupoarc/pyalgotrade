
from __future__ import print_function

import hmac, hashlib, time
from urllib3.util import parse_url

import requests
import ujson as json
from requests.auth import AuthBase

from ..broker import FloatTraits
from ..orderbook import Ask, Bid, Assign, MarketSnapshot
from . import VENUE, DEFAULT_SYMBOL, LOCAL_SYMBOL


def flmath(n):
    return round(n, 12)

def fees(txnsize):
    return flmath(txnsize * float('0.0025'))

# ---------------------------------------------------------------------------
#  Binance market data message helper / decoder
# ---------------------------------------------------------------------------


def toBookMessages(binance_json, symbol):
    """convert a binance json message into a list of book messages"""
    m = binance_json
    if type(m) != dict:
        m = json.loads(m)

    if m['e'] != "depthUpdate":
        raise ValueError("Unknown binance event type: %r" % m)

    rts = m.get("u") or m.get('lastUpdateId')
    # their REST book endpoint (depth) doesn't give a time,it give a 'lastUpdateID'
    # while their streamingupdates give both.  But we have to sync, so we use the
    # updateID to sync on, so it becomes our clock

    msgs = []
    for a in m['a']:
        msgs.append(Assign(rts, VENUE, symbol, float(a[0]), float(a[1]), Ask))
    for b in m['b']:
        msgs.append(Assign(rts, VENUE, symbol, float(b[0]), float(b[1]), Bid))

    return msgs


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



# ---------------------------------------------------------------------------
#  Binance authentication helpers
# ---------------------------------------------------------------------------

class BinanceAuth(AuthBase):
    """a requests-module-compatible auth module"""
    def __init__(self, key, secret):
        self.api_key    = key
        self.secret_key = secret

    def __call__(self, request):
        # all auths need a header set
        request.headers.update({
            'X-MBX-APIKEY': self.api_key
        })
        return request


class BinanceSign(BinanceAuth):

    RECV_WINDOW = 5000

    def __call__(self, request):
        super(BinanceSign, self).__call__(request)

        def signature(message):
            return hmac.new(self.secret_key.encode('utf-8'), message, hashlib.sha256).hexdigest()

        # put the required timestamp into the data
        timestamp = str(int(time.time()*1000))
        if request.method == 'POST':
            print("Got POST")
            request.data = getattr(request, 'data', {})
            request.data['timestamp'] = timestamp
            request.data['recvWindow'] = self.RECV_WINDOW
            request.prepare_body(request.data, [])
            request.data['signature'] = signature(request.body)
            request.prepare_body(request.data, [])

        else:
            request.params = getattr(request, 'params', {})
            request.params['timestamp'] = timestamp
            request.params['recvWindow'] = self.RECV_WINDOW
            request.prepare_url(request.url, request.params)
            scheme, auth, host, port, path, query, fragment = parse_url(request.url)
            request.prepare_url(request.url, { 'signature': signature(query) })
        return request


# ---------------------------------------------------------------------------
#  Binance REST client
# ---------------------------------------------------------------------------

URL = "https://api.binance.com/api/"

from attrdict import AttrDict

BinanceOrder = AttrDict

#BinanceOrder = namedtuple('BinanceOrder', 'id size price done_reason status settled filled_size executed_value product_id fill_fees side created_at done_at')
#BinanceOrder.__new__.__defaults__ = (None,) * len(BinanceOrder._fields)

class BinanceRest(object):

    # Time in Force
    GTC = GOOD_TIL_CANCEL = object()
    IOC = IMMEDIATE_OR_CANCEL = object()
    FOK = FILL_OR_KILL = object()

    GTT = GOOD_TIL_TIME = object()
    POST_ONLY = object()

    def __init__(self, key, secret):
        self.__auth = BinanceAuth(key, secret)
        self.__sign = BinanceSign(key, secret)

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
            except Exception:
                print("ERROR: " + method + " " + url + " " + repr(kwargs) + " GOT: " + result.text)
                raise
        return result

    def _auth_request(self, method, url, **kwargs):
        if not 'auth' in kwargs: kwargs['auth'] = self.__auth
        return self._request(method, url, **kwargs)

    def _sign_request(self, method, url, **kwargs):
        if not 'auth' in kwargs: kwargs['auth'] = self.__sign
        return self._request(method, url, **kwargs)

    def _get(self, url, **kwargs): return self._request('GET', url, **kwargs)
    def _getj(self, url, **kwargs): return self._get(url, **kwargs).json()
    def _auth_getj(self, url, **kwargs): return self._auth_request('GET', url, **kwargs).json()
    def _auth_postj(self, url, **kwargs): return self._auth_request('POST', url, **kwargs).json()
    def _auth_delj(self, url, **kwargs): return self._auth_request('DELETE', url, **kwargs).json()
    def _sign_getj(self, url, **kwargs): return self._sign_request('GET', url, **kwargs).json()
    def _sign_postj(self, url, **kwargs): return self._sign_request('POST', url, **kwargs).json()
    def _sign_delj(self, url, **kwargs): return self._sign_request('DELETE', url, **kwargs).json()


    #
    # Public endpoints
    #

    # ping

    def server_time(self):
        return self._getj('v1/time')

    def exchange_info(self):
        return self._getj('v1/exchangeInfo')

    def book(self, symbol=DEFAULT_SYMBOL, limit=100):
        return self._getj('v1/depth', params={ 'symbol': symbol, 'limit': limit })

    def trades(self, symbol=DEFAULT_SYMBOL, limit=100):
        return self._getj('v1/trades', params={ 'symbol': symbol, 'limit': limit })


    #
    # Account (private endpoints)
    #

    def account(self):
        return self._sign_getj('v3/account')

    def order(self, symbol, orderId):
        return self._sign_getj('v3/order', params={ 'symbol': LOCAL_SYMBOL[symbol], 'orderId' : orderId })

    def cancel(self, symbol, orderId):
        return self._sign_delj('v3/order', params={ 'symbol': LOCAL_SYMBOL[symbol], 'orderId' : orderId })

    def open_orders(self, symbol=None):
        params = {'symbol': LOCAL_SYMBOL[symbol]} if symbol is not None else {}
        return self._sign_getj('v3/openOrders', params=params)

    #
    # Cooked endpoints
    #

    def balances(self):
        return { j['asset']: float(j['free']) for j in self.account().get('balances',[]) }

    def tradeable(self):
        results = []
        for s in self.exchange_info()['symbols']:
            results.append(s['baseAsset'] + s['quoteAsset'])
        return results

    def open_Orders(self, *a, **kw):
        return  [ BinanceOrder(**o) for o in self.open_orders(*a, **kw) ]

    def book_snapshot(self, symbol=DEFAULT_SYMBOL):
        book = self.book(symbol)
        def mkassign(ts, price, size, side):
            return Assign(ts, VENUE, symbol, price, size, side)
        rts = book['lastUpdateId']
        price = lambda e: float(e[0])
        size = lambda e: float(e[1])
        return MarketSnapshot(time.time(), VENUE, symbol,
            [ mkassign(rts, price(e), size(e), Bid) for e in book['bids'] ] +
            [ mkassign(rts, price(e), size(e), Ask) for e in book['asks'] ]
        )

    def instrumentTraits(self):
        return { s['symbol']: FloatTraits(s['baseAssetPrecision'], s['quotePrecision']) for s in self.exchange_info()['symbols'] }

