
from __future__ import print_function

import hmac, hashlib, time, requests, base64, threading, Queue
import base64
import ujson as json

from requests.auth import AuthBase
from pyalgotrade.orderbook import Increase, Decrease, Ask, Bid, Assign, MarketSnapshot
import pyalgotrade.logger

logger = pyalgotrade.logger.getLogger("kraken")

BTCUSD, BTCEUR = 'BTCUSD', 'BTCEUR'

LOCAL_SYMBOL = { BTCUSD: 'XXBTZUSD', BTCEUR: 'XXBTZEUR' }
SYMBOL_LOCAL = { v: k for k, v in LOCAL_SYMBOL.items() }
SYMBOLS = list(LOCAL_SYMBOL.keys())
VENUE = 'kraken'


def flmath(n):
    return round(n, 12)

def fees(txnsize):
    return flmath(txnsize * float('0.0025'))


# ---------------------------------------------------------------------------
#  Kraken market data message helper / decoder
# ---------------------------------------------------------------------------


def toBookMessages(coinbase_json, symbol):
    """convert a kraken json message into a list of book messages"""
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

from attrdict import AttrDict

KrakenOrder = AttrDict

#KrakenOrder = namedtuple('KrakenOrder', 'pair side type price price2 volume leverage oflags starttm expiretm userref close_type close_price close_price2')
#KrakenOrder.__new__.__defaults__ = (None,) * len(KrakenOrder._fields)

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
            except Exception:
                print("ERROR: " + method + " " + url + " " + repr(kwargs) + " GOT: " + result.text)
                raise
        return result

    def _auth_request(self, method, url, **kwargs):
        if not 'auth' in kwargs: kwargs['auth'] = self.auth()
        return self._request(method, url, **kwargs)

    def _get(self, url, **kwargs): return self._request('GET', url, **kwargs).json()
    def _getj(self, url, **kwargs): return self._get(url + 'public/', **kwargs).json()
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
        return self._getj('AssetPairs')

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
        return self._auth_postj('OpenOrders', data=params)['result']['open']

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
    #def query_orders(self, *txids, trades=False, userref=None):
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





    def place_order(self, pair, side, otype, size, price=None, price2=None, leverage=None, oflags=None, starttm=0, expiretm=0, userref=None, validate=False, closeorder=None):
        params = {
            'pair': pair,
            'type': { Bid: "buy", Ask: "sell" }[side],
            'ordertype': otype,
            'volume': size
        }
        LOCAL_FLAGS = { self.POST_ONLY: 'post' }
        if price is not None: params['price'] = price
        if price2 is not None: params['price2'] = price2
        if leverage is not None: params['leverage'] = leverage
        if oflags is not None: params['oflags'] = ','.join(LOCAL_FLAGS[f] for f in oflags)
        if starttm is not None: params['starttm'] = starttm
        if expiretm is not None: params['expiretm'] = expiretm
        if userref is not None: params['userref'] = userref
        if closeorder is not None: params['close'] = closeorder
        if validate: params['validate'] = 'true'
        return self._auth_postj('AddOrder', data=params)

    def cancel(self, txid):
        params = { 'txid': txid }
        return self._auth_postj('CancelOrder', data=params)


#   def Order(self, id):
#       return KrakenOrder(**(self.order(id)))


# impedence match to the rest of the system

    def balances(self):
        return self.accounts()['result']

    def limitorder(self, side, price, size, symbol=BTCUSD, flags=()):
        # newOrderId = self.__httpClient.limitorder(side, price, size, flags=flags)
        result = self.place_order(LOCAL_SYMBOL[symbol], side, 'limit', size, price=price)
        if result['error']:
            raise Exception(str(result['error']))
        return result['result']['txid'][0]

    def marketorder(self, side, size, symbol=BTCUSD):
        #newOrderId = self.__httpClient.marketorder(side, size)
        result = self.place_order(LOCAL_SYMBOL[symbol], side, 'market', size)
        if result['error']:
            raise Exception(str(result['error']))
        return result['result']['txid'][0]

    def book_snapshot(self, symbol=BTCUSD):
        ksymbol = LOCAL_SYMBOL[symbol]
        book = self.book(ksymbol)['result']

        def mkassign(ts, price, size, side):
            return Assign(ts, VENUE, symbol, price, size, side)

        return MarketSnapshot(time.time(), VENUE, symbol,
            [ mkassign(rts, float(price), float(size), Bid) for price, size, rts in book[ksymbol]['bids'] ] +
            [ mkassign(rts, float(price), float(size), Ask) for price, size, rts in book[ksymbol]['asks'] ]
        )

    def inside_bid_ask(self, symbol=BTCUSD):
        ksymbol = LOCAL_SYMBOL[symbol]
        book = self.book(ksymbol, 1)['result'][ksymbol]
        bid = book['bids'][0][0]
        ask = book['asks'][0][0]
        #log.info("Got inside bid: {} ask: {}".format(bid, ask))
        return bid, ask

    def OpenOrders(self, **ooargs):
        return [KrakenOrder(txid, **oinfo) for txid, oinfo in self.open_orders(**ooargs).items()]




class BookPoller(threading.Thread):

    ON_ORDER_BOOK_UPDATE = object()

    def __init__(self, httpClient, poll_frequency=1):
        super(BookPoller, self).__init__()
        self.__httpClient = httpClient
        self.poll_frequency = poll_frequency
        self.__queue = Queue.Queue()
        self.__running = True

    def _poll(self):
        return [(self.ON_ORDER_BOOK_UPDATE, self.__httpClient.book_snapshot())]

    def getQueue(self):
        return self.__queue

    def get(self, *a, **kw):
        return self.__queue.get(*a, **kw)

    def run(self):
        while self.__running:
            try:
                events = self._poll()
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
        return self.__running and super(BookPoller, self).is_alive()

    def join(self):
        if self.is_alive():
            super(BookPoller, self).join()

