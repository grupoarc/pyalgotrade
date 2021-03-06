
from __future__ import print_function

import hmac, hashlib, time, requests, base64
import ujson as json

from pyalgotrade import Symbol
from requests.auth import AuthBase
from pyalgotrade.orderbook import Increase, Decrease, Ask, Bid, Assign, MarketSnapshot

from . import LOCAL_SYMBOL, VENUE

def flmath(n):
    return round(n, 12)

def fees(txnsize):
    return flmath(txnsize * float('0.0025'))


# ---------------------------------------------------------------------------
#  Coinbase market data message helper / decoder
# ---------------------------------------------------------------------------


def toBookMessages(coinbase_json, symbol):
    """convert a coinbase json message into a list of book messages"""
    cbase = coinbase_json
    if type(cbase) != dict:
        cbase = json.loads(cbase)
    cbt = cbase['type']
    if cbt == 'received':
        return []
    if cbt == 'done' and cbase['order_type'] == 'market':
        return []
    side = { 'buy': Bid, 'sell': Ask }.get(cbase['side'], None)
    if side is None: raise ValueError("Unknown side %r" % cbase['side'])
    if not 'price' in cbase: return [] #change of a market order
    price = cbase['price']
    if cbt == 'done':
        mtype, size = Decrease, cbase['remaining_size']
    elif cbt == 'open':
        mtype, size = Increase, cbase['remaining_size']
    elif cbt == 'match':
        mtype, size = Decrease, cbase['size']
    elif cbt == 'change':
        if price == 'null': return []
        mtype = Decrease
        size = flmath(float(cbase['old_size']) - float(cbase['new_size']))
    else:
        raise ValueError("Unknown coinbase message: %r" % cbase)
    #rts = datetime.strptime(cbase['time'], "%Y-%m-%dT%H:%M:%S.%fZ")
    rts = int(cbase['sequence'])
    return [mtype(rts, VENUE, symbol, float(price), float(size), side)]


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

URL = "https://api.gdax.com/"

from attrdict import AttrDict

CoinbaseOrder = AttrDict

#CoinbaseOrder = namedtuple('CoinbaseOrder', 'id size price done_reason status settled filled_size executed_value product_id fill_fees side created_at done_at')
#CoinbaseOrder.__new__.__defaults__ = (None,) * len(CoinbaseOrder._fields)

class CoinbaseRest(object):

    GTC = GOOD_TIL_CANCEL = object()
    GTT = GOOD_TIL_TIME = object()
    IOC = IMMEDIATE_OR_CANCEL = object()
    FOK = FILL_OR_KILL = object()
    POST_ONLY = object()

    def __init__(self, key, secret, passphrase):
        self.__auth = CoinbaseAuth(key, secret, passphrase)

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
        if not 'auth' in kwargs: kwargs['auth'] = self.auth()
        return self._request(method, url, **kwargs)

    def _get(self, url, **kwargs): return self._request('GET', url, **kwargs)
    def _getj(self, url, **kwargs): return self._get(url, **kwargs).json()
    def _auth_getj(self, url, **kwargs): return self._auth_request('GET', url, **kwargs).json()
    def _auth_postj(self, url, **kwargs): return self._auth_request('POST', url, **kwargs).json()
    def _auth_delj(self, url, **kwargs): return self._auth_request('DELETE', url, **kwargs).json()

    #
    # Market data (public endpoints)
    #

    def products(self):
        return self._getj('products')

    def stats(self, symbol=Symbol.BTC_USD):
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
        return self._auth_getj('orders/' + str(id))

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
        return self._auth_postj('orders', json=params)

    def limitorder(self, side, price, size, symbol=Symbol.BTC_USD, flags=(), cancel_after=None):
        """Place a limit order"""
        bs = { Bid: "buy", Ask: "sell" }[side]
        params = {
            'type' : 'limit',
            'side' : bs,
            'product_id' : LOCAL_SYMBOL[symbol],
            'price' : price,
            'size' : size
            }
        if self.GTT in flags:
            if cancel_after is None: raise ValueError("No cancel time specified")
            params['time_in_force'] = 'GTT'
            params['cancel_after'] = cancel_after
        if self.POST_ONLY in flags: params['post_only'] = True
        elif not self.GTT in flags:
            if self.GTC in flags: params['time_in_force'] = 'GTC'
            elif self.IOC in flags: params['time_in_force'] = 'IOC'
            elif self.FOK in flags: params['time_in_force'] = 'FOK'

        return self._auth_postj('orders', json=params)['id']

    def marketorder(self, side, size, symbol=Symbol.BTC_USD):
        """Place a market order"""
        bs = { Bid: "buy", Ask: "sell" }[side]
        params = {
            'type' : 'market',
            'side' : bs,
            'product_id' : LOCAL_SYMBOL[symbol],
            'size' : size
            }
        return self._auth_postj('orders', json=params)['id']

    def cancel(self, orderId=None, raise_errors=False):
        url = 'orders'
        if orderId is not None: url += '/' + orderId
        return self._auth_delj(url, raise_errors=raise_errors)

    def book(self, symbol=Symbol.BTC_USD, level=2, raw=False):
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

    def book_snapshot(self, symbol=Symbol.BTC_USD):
        book = self.book(symbol)
        def mkassign(ts, price, size, side):
            return Assign(ts, VENUE, symbol, price, size, side)
        rts = book['sequence']
        price = lambda e: float(e[0])
        size = lambda e: float(e[1])
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

