
from __future__ import print_function

import sys
import time
import urlparse
import posixpath
import json
from decimal import Decimal
import hmac, hashlib, requests
from collections import namedtuple

from requests.auth import AuthBase

#from .mdc import UpdatingMDC
from .book import Assign, Bid, Ask, MarketSnapshot, MarketUpdate
#from .book import LimitOrder, MarketOrder
#from .book import Balances, Balance

BTCUSD='BTCUSD'

VENUE = "bitfinex"
# supported as of 2016-05-23: BTCUSD, LTCUSD, LTCBTC, ETHUSD, ETHBTC
LOCAL_SYMBOL = { BTCUSD: 'BTCUSD' }
SYMBOL_LOCAL = { v:k for k, v in LOCAL_SYMBOL.items() }
SYMBOLS = list(LOCAL_SYMBOL.keys())


def fees(txnsize):
    return txnsize * Decimal('0.0025')


def toBookMessages(msg, symbol):
    """takes a book message and returns a list of Market* messages
    either [[[p1, c1, s1], [p2, c2, s2], ...]] (a snapshot)
    or [p1, c1, s1] (an update)
    if s is positive, they're bids, if negative, asks
    """
    result = []
    now = time.time()
    if len(msg) > 1:
        msg = [[ msg ]]
    for price, _, size  in msg[0]:
        side = Bid if size > 0 else Ask
        result.append(Assign(now, VENUE, symbol, price, abs(size), side))
    return result


def toMarketMessage(msg, symbol):
    # Note: this doesn't take the *whole* message, it takes [1:] of the update,
    # which skips the channel ID
    tvs = { 'ts': time.time(), 'venue': VENUE, 'symbol':symbol }
    if len(msg) < 2: mtype = MarketSnapshot
    else: mtype = MarketUpdate
    return mtype(data=toBookMessages(msg, symbol), **tvs)

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


def singleton(cls):
    instances = {}
    def getinstance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return getinstance



# Bitfinex auth module
class BitfinexAuth(AuthBase):
    """a requests-module-compatible auth module"""
    def __init__(self):
        from .secrets import bitfinex as b
        self.api_key    = b.api_key
        self.api_secret = b.api_secret

    def __call__(self, request):
        nonce = str(int(time.time()* 100))
        payload =  { 'request': request.path_url,
                     'nonce': nonce,
                    }

        if request.body:
            data = urlparse.parse_qs(request.body)
            for k in data:
                payload[k] = data[k][0]

        payload = json.dumps(payload).encode('base64').rstrip('\n')

        signature = hmac.new(self.api_secret, payload, hashlib.sha384).hexdigest()

        request.headers.update({
            'X-BFX-APIKEY': self.api_key,
            'X-BFX-PAYLOAD': payload,
            'X-BFX-SIGNATURE': signature
        })
        request.body = payload
        return request


URL='https://api.bitfinex.com/v1'



BitfinexOrder = namedtuple('BitfinexOrder', 'id symbol exchange price avg_execution_price side type timestamp is_live is_cancelled is_hidden oco_order was_forced executed_amount remaining_amount original_amount')
BitfinexOrder.__new__.__defaults__ = (None,) * len(BitfinexOrder._fields)


@singleton
class RESTClient:

    def __init__(self, api_key, api_secret):
        self.auth = BitfinexAuth(api_key, api_secret)

    @property
    @lazy_init
    def _session(self):
        return requests.Session()

    def _request(self, method, *url, **kwargs):
        result = self._session.request(method, posixpath.join(URL, *url), **kwargs)
        result.raise_for_status() # raise if not status == 200
        return result

    def _auth_request(self, method, url, **kwargs):
        if not 'auth' in kwargs: kwargs['auth'] = self.auth
        return self._request(method, url, **kwargs)

    def _get(self, *url, **kwargs): return self._request('get', *url, **kwargs)
    def _getj(self, *url, **kwargs): return self._get(*url, **kwargs).json()
    def _auth_getj(self, *url, **kwargs): return self._auth_request('get', *url, **kwargs).json()
    def _auth_post(self, *url, **kwargs): return self._auth_request('post', *url, **kwargs)
    def _auth_postj(self, *url, **kwargs): return self._auth_request('post', *url, **kwargs).json()
    def _auth_delj(self, *url, **kwargs): return self._auth_request('delete', *url, **kwargs).json()

    def raw_balances(self):
        return self._auth_postj('balances')

    def balances(self):
        return { b['currency'].upper() : float(b['available']) for b in self.raw_balances()
                 if b['type'] == 'exchange' }

    def book(self, symbol=BTCUSD, raw=False):
        book = self._get('book', LOCAL_SYMBOL[symbol])
        if raw: return book.text
        else: return book.json()

    def book_snapshot(self, symbol=BTCUSD):
        def mkassign(ts, price, size, side):
            return Assign(ts, VENUE, symbol, price, size, side)
        rts = lambda e: e['timestamp']
        price = lambda e: Decimal(e['price'])
        size = lambda e: Decimal(e['amount'])
        book = self.book(symbol)
        return MarketSnapshot(time.time(), VENUE, symbol,
            [ mkassign(rts(e), price(e), size(e), Bid) for e in book['bids'] ] +
            [ mkassign(rts(e), price(e), size(e), Ask) for e in book['asks'] ]
        )

    def order(self, side, price, size, symbol=BTCUSD, type='limit', hidden=False, postonly=False):
        r = {
            'symbol': LOCAL_SYMBOL[symbol],
            'amount': size,
            'price': price,
            'exchange': 'bitfinex',
            'side': side,
            'type': type,
        }
        if hidden: r['is_hidden'] = 'true'
        if postonly: r['is_postonly'] = 'true'
        return self._auth_postj('order', 'new', data=r)

    def Order(self, *args, **kwargs):
        j = self.order(*args, **kwargs)
        return BitfinexOrder(**j)

    limitorder = Order

    def orders(self):
        return self._auth_postj('orders')

    def cancel(self, oid):
        r = { 'order_id': int(oid) }
        return self._auth_postj('order', 'cancel', data=r)

    def order_multi(self, orders):
        # orders is an iterable of {Limit,Market}Order objects
        data = []
        for o in orders:
            otype = {LimitOrder: 'limit', MarketOrder: 'market'}[type(o)]
            data.append({'symbol': LOCAL_SYMBOL[o.symbol],
                         'amount': o.size,
                         'price': o.price,
                         'exchange': o.venue,
                         'side': { Bid: 'buy', Ask: 'sell' }[o.side],
                         'type': otype
                         })
        return self._auth_postj('order/new/multi', data={'orders': data})


    def order_statuses(self):
        os = []
        for o in self.orders():
            detail = { 'ts': time.time(),
                      'rts': o['timestamp'], # TODO: convert this to std timeformat or something
                      'id': o['id'],
                      'symbol': SYMBOL_LOCAL[o['symbol']],
                      'venue': VENUE,
                      'side': {'buy':Bid, 'sell':Ask}[o['side']],
                      'price': Decimal(o['price']),
                      'size': Decimal(o['original_amount']),
                      'filled': Decimal(o['executed_amount']),
                      }
            os.append(Active(**detail))
        return OrderStatuses(time.time(), None, VENUE, os)

    def cancel_all_orders(self):
        """Cancel all outstanding orders"""
        return self._auth_post('order/cancel/all')


