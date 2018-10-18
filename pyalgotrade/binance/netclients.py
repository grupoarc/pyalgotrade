
from __future__ import print_function

import hmac, hashlib, time
from urllib3.util import parse_url
from datetime import datetime

import requests
import ujson as json
from attrdict import AttrDict
from requests.auth import AuthBase
from sortedcontainers import SortedList

from .. import logger as pyalgo_logger
from ..utils import memoize as lazy_init
from ..broker import FloatTraits, Order, LimitOrder, MarketOrder, OrderExecutionInfo
from ..orderbook import Ask, Bid, Assign, MarketSnapshot
from . import VENUE, DEFAULT_SYMBOL, LOCAL_SYMBOL, SYMBOL_LOCAL

logger = pyalgo_logger.getLogger(__name__)

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

try:
    # Python 3
    from urllib.parse import parse_qs
except ImportError:
    # Python 2
    from urlparse import parse_qs


URL_ENCODED = 'application/x-www-form-urlencoded'

class BinanceSign(BinanceAuth):

    RECV_WINDOW = 5000

    def __call__(self, request):
        super(BinanceSign, self).__call__(request)

        def signature(message):
            return hmac.new(self.secret_key.encode('utf-8'), message, hashlib.sha256).hexdigest()

        # put the required timestamp into the data
        timestamp = str(int(time.time()*1000))

        if request.body: # POST
            assert request.headers.get('Content-Type') == URL_ENCODED
            data = parse_qs(request.body)
            data['timestamp'] = timestamp
            data['recvWindow'] = self.RECV_WINDOW
            request.prepare_body(data, None, None)
            data['signature'] = signature(request.body)
            request.prepare_body(data, None, None)
        else:
            params = getattr(request, 'params', {})
            params['timestamp'] = timestamp
            params['recvWindow'] = self.RECV_WINDOW
            request.prepare_url(request.url, params)
            scheme, auth, host, port, path, query, fragment = parse_url(request.url)
            request.prepare_url(request.url, { 'signature': signature(query) })
        return request


# ---------------------------------------------------------------------------
#  Binance REST client
# ---------------------------------------------------------------------------

URL = "https://api.binance.com/api/"

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

    @lazy_init
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

    def order(self, symbol, side, otype, size, extra={}):
        data = {'symbol': LOCAL_SYMBOL[symbol],
                'side': { Bid: "BUY", Ask: "SELL" }[side],
                'type': otype,
                'quantity': size }
        data.update(extra)
        return self._sign_postj('v3/order', data=data)

    def cancel(self, symbol, orderId):
        return self._sign_delj('v3/order', data={ 'symbol': LOCAL_SYMBOL[symbol], 'orderId' : orderId })

    def open_orders(self, symbol=None):
        params = {'symbol': LOCAL_SYMBOL[symbol]} if symbol is not None else {}
        return self._sign_getj('v3/openOrders', params=params)

    def all_orders(self, symbol, min_orderId=None, startTime=None, endTime=None, limit=1000):
        params = {'symbol': LOCAL_SYMBOL[symbol], 'limit': limit}
        if min_orderId is not None: params['orderId'] = min_orderId
        if startTime is not None: params['startTime'] = startTime
        if endTime is not None: params['endTime'] = endTime
        return self._sign_getj('v3/allOrders', params=params)

    def all_orders_full(self, symbol, startTime, endTime=None):
        LIMIT_BY=1000
        result, received = [], None
        while received is None or len(received) == LIMIT_BY:
            min_id = None if received is None else received[-1]['orderId'] + 1
            received = self.all_orders(symbol, min_orderId=min_id, startTime=startTime, endTime=endTime, limit=LIMIT_BY)
            result += received
        return result

    def my_trades(self, symbol, minId=None, startTime=None, endTime=None, limit=1000):
        params = {'symbol': LOCAL_SYMBOL[symbol], 'limit': limit}
        if minId is not None: params['fromId'] = minId
        if startTime is not None: params['startTime'] = startTime
        if endTime is not None: params['endTime'] = endTime
        return self._sign_getj('v3/myTrades', params=params)

    def my_trades_full(self, symbol, startTime, endTime=None):
        LIMIT_BY=1000
        result, received = [], None
        while received is None or len(received) == LIMIT_BY:
            min_id = None if received is None else received[-1]['id'] + 1
            received = self.my_trades(symbol, minId=min_id, startTime=startTime, endTime=endTime, limit=LIMIT_BY)
            result += received
        return result

    #
    # Cooked endpoints
    #

    def balances(self):
        return { j['asset']: float(j['free']) for j in self.account().get('balances',[]) }

    def tradeable(self):
        return ( s['baseAsset'] + s['quoteAsset'] for s in self.exchange_info()['symbols'] )

    def cancelOrder(self, order):
        return self.cancel(order.getInstrument(), order.getId())

    def OpenOrders(self, symbol=None):
        return [self._order_to_Order(oinfo) for oinfo in self.open_orders(symbol)]

    def _order_to_Order(self, oinfo, oei_info={}):
        logger.debug("making Order from {!r}".format(oinfo,))
        oinfo = AttrDict(oinfo)
        action = { 'BUY': Order.Action.BUY,
                  'SELL': Order.Action.SELL
                 }.get(oinfo.side)
        if action is None:
            raise Exception("Invalid order side")
        size = float(oinfo.origQty)
        symbol =  SYMBOL_LOCAL[oinfo.symbol]
        instrumentTraits = self.instrumentTraits()[symbol]

        if oinfo.type == 'LIMIT': # limit order
            price = float(oinfo.price)
            o = LimitOrder(action, symbol, price, size, instrumentTraits)
        elif oinfo.type  == 'MARKET':  # Market order
            onClose = False # TBD
            o = MarketOrder(action, symbol, size, onClose, instrumentTraits)
        else:
            raise ValueError("Unsuported Ordertype: " + oinfo['type'])

        submit_time = datetime.fromtimestamp(float(oinfo['time']/1000))
        o.setSubmitted(oinfo['orderId'], submit_time)

        # status is one of:
        # NEW, PARTIALLY_FILLED, FILLED, CANCELED, PENDING_CANCEL (currently unused), REJECTED, EXPIRED
        has_oei = False
        if oinfo.status == 'NEW':
            o.setState(Order.State.ACCEPTED)
        elif oinfo.status == 'PARTIALLY_FILLED':
            has_oei = True
            o.setState(Order.State.ACCEPTED)
        elif oinfo.status == 'FILLED':
            has_oei = True
            o.setState(Order.State.ACCEPTED) # adding OEI info below will make it filled
        elif oinfo.status in ('CANCELED', 'EXPIRED', 'REJECTED'):
            o.setState(Order.State.CANCELED)
        else:
            raise ValueError("Unsuported Order status: " + oinfo.status)

        if oinfo.orderId in oei_info:
            for oei in oei_info[oinfo.orderId]:
                o.addExecutionInfo(oei)
        else:
            qty = float(oinfo.get('executedQty', 0))
            if qty > 0 or has_oei:
                price = float(oinfo.price)
                update_tm = datetime.fromtimestamp(float(oinfo.updateTime/1000))
                o.addExecutionInfo(OrderExecutionInfo(price, qty, 0, update_tm))

        return o

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
        trait = lambda td: FloatTraits(td['baseAssetPrecision'], td['quotePrecision'])
        r = { SYMBOL_LOCAL[s['symbol']]: trait(s)
                for s in self.exchange_info()['symbols'] if s['symbol'] in SYMBOL_LOCAL }
        logger.debug("instrument traits: {!r}".format(r))
        return r

    def limitorder(self, side, price, size, symbol, flags=()):
        extra = { 'price' : price }
        for flag, val in { self.GTC: "GTC", self.IOC: "IOC", self.FOK: "FOK" }.items():
            if flag in flags:
                extra['timeInForce'] = val
        o = self.order(symbol, side, "LIMIT", size, extra=extra)
        return o['orderId']

    def marketorder(self, side, size, symbol, flags=()):
        extra = {}
        for flag, val in { self.GTC: "GTC", self.IOC: "IOC", self.FOK: "FOK" }.items():
            if flag in flags:
                extra['timeInForce'] = val
        o = self.order(symbol, side, "MARKET", size, extra=extra)
        return o['orderId']

    def _trade_to_OEI(self, info):
        price = float(info['price'])
        quantity = float(info['qty'])
        commission = float(info['commission'])
        timestamp = datetime.fromtimestamp(info['time']/1000.0)
        return OrderExecutionInfo(price, quantity, commission, timestamp)

    @lazy_init
    def OrderExecutionInfo(self, since, symbols=None):

        if symbols is None:
            symbols = set(SYMBOL_LOCAL[s] for s in self.tradeable())

        oei_info = {}
        for symbol in symbols:
            for t in self.my_trades_full(symbol, startTime=since):
                oid = t['orderId']
                oei = self._trade_to_OEI(t)
                if oid in oei_info:
                    oei_info[oid].append(oei)
                else:
                    oei_info[oid] = [oei]
        return oei_info


    def ClosedOrders(self, since, symbols=None):
        """Return a list of filled Orders newer than <since>
        since is a unix timestamp
        symbol is a list of Symbols, or None for all
        """

        if symbols is None:
            symbols = set(SYMBOL_LOCAL[s] for s in self.tradeable())

        orders = SortedList(key=lambda o: o.getSubmitDateTime())
        oei_info = self.OrderExecutionInfo(since, symbols)
        for sym in symbols:
            orders.update(self._order_to_Order(o, oei_info) for o in self.all_orders_full(sym, since))

        return list(orders)

