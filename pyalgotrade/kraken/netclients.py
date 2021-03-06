
from __future__ import print_function

from datetime import datetime
import hmac, hashlib, time, requests, base64, threading, Queue
#import ujson as json

from attrdict import AttrDict
from requests.auth import AuthBase

from .. import broker
from .. import logger as pyalgo_logger
from ..utils import memoize as lazy_init
from ..broker import FloatTraits, OrderExecutionInfo
from ..orderbook import Ask, Bid, Assign, MarketSnapshot

from . import VENUE, LOCAL_SYMBOL, SYMBOL_LOCAL, SYMBOLS

logger = pyalgo_logger.getLogger(__name__)



def flmath(n):
    return round(n, 12)

def fees(txnsize):
    return flmath(txnsize * float('0.0025'))


# ---------------------------------------------------------------------------
#  Kraken authentication helper
# ---------------------------------------------------------------------------

try:
    # Python 3
    from urllib.parse import parse_qs
except ImportError:
    # Python 2
    from urlparse import parse_qs


URL_ENCODED = 'application/x-www-form-urlencoded'

class KrakenAuth(AuthBase):
    """a requests-module-compatible auth module for kraken.com"""
    def __init__(self, key, secret):
        self.api_key    = key
        self.secret_key = secret

    def __call__(self, request):
        if request.body:
            assert request.headers.get('Content-Type') == URL_ENCODED
            data = parse_qs(request.body)
        else:
            data = {}

        nonce = int(1000 * time.time())

        # insert the nonce in the encoded body
        data['nonce'] = nonce
        request.prepare_body(data, None, None)

        body = request.body
        if not isinstance(body, bytes):   # Python 3
            body = body.encode('latin1')  # standard encoding for HTTP

        message = request.path_url + hashlib.sha256(b'%s%s' % (nonce, body)).digest()
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha512).digest()
        signature = base64.b64encode(signature)

        request.headers.update({
            'API-Key': self.api_key,
            'API-Sign': signature
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
        params = { 'ofs': offset, 'trades': 'true' if trades else 'false', 'closetime': closetime or 'both' }
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
            'volume': str(size)
        }
        LOCAL_FLAGS = { self.POST_ONLY: 'post' }
        for k in ('price', 'price2', 'leverage', 'starttm', 'expiretm', 'userref', 'close', 'validate'):
            if k in kwargs: params[k] = str(kwargs[k])
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
        result = self.place_order(LOCAL_SYMBOL[symbol], side, 'limit', size, price=price, oflags=flags)
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
        trait = lambda td: FloatTraits(td['lot_decimals'], td['pair_decimals'])
        return { SYMBOL_LOCAL[p]: trait(d)
                   for p, d in self.asset_pairs().items() if p in SYMBOL_LOCAL }

    def _order_to_Order(self, txid, oinfo, get_oei_info):
        korder = AttrDict(oinfo)
        logger.debug("got krakenorder {!r}: {!r}".format(txid, korder))
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
            # Not sure which is more correct here, but refid isn't defined for open limit orders
            o.setSubmitted(txid, datetime.fromtimestamp(korder.opentm))
            #o.setSubmitted(korder.refid, korder.opentm)

        newstate = {'pending': broker.Order.State.SUBMITTED,
                    'open': broker.Order.State.ACCEPTED,
                    'closed': broker.Order.State.ACCEPTED,  # because we're going to mock OEI to fill it so we can get an avgFillPrice
                    'canceled': broker.Order.State.CANCELED,
                    'expired': broker.Order.State.CANCELED
                    }[korder.status]
        if korder.status == 'open' and korder.get('vol_exc', 0) > 0:
            newstate = broker.Order.State.PARTIALLY_FILLED
        o.setState(newstate)

        tradetime2dt = lambda t: datetime.fromtimestamp(t/1000)

        if korder.status == 'closed':
            closetime = tradetime2dt(korder.get('closetm', korder.get('starttm')))
            o.addExecutionInfo(OrderExecutionInfo(float(korder.price), float(korder.vol), float(korder.fee), closetime))
        else:
            tradelist = korder.get('trades')
            if tradelist is None:
                pass
            elif type(tradelist) == dict:
                for ttid, tinfo in tradelist:
                    o.addExecutionInfo(OrderExecutionInfo(tinfo.price, tinfo.vol, tinfo.fee, tradetime2dt(tinfo.time)))
            else: # list
                oei_info = get_oei_info()
                for ttid in tradelist:
                    if ttid in oei_info:
                        o.addExecutionInfo(oei_info[ttid])
        return o

    def OpenOrders(self):
        return [self._order_to_Order(txid, oinfo) for txid, oinfo in self.open_orders(trades=True).items()]

    def _fetch_all(self, fetchlambda, piecekey):
        """assemble a full result from kraken's weird pagination
           fetchlambda = a function that takes an offset and does a request for that offset from kraken
           piecekey - the key in te result that has the partial results
        """
        results, count = None, None
        while count is None or len(results) < count:
            offset = 0 if results is None else len(results)
            piece = fetchlambda(offset)['result']
            count = piece['count']
            result = piece[piecekey]
            if type(result) == dict:
                if results is None: results = {}
                results.update(result)
            else:
                if results is None: results = []
                results += result
        return result

    def _trade_to_OEI(self, info, txid):
        price = info['price']
        quantity = info['vol']
        commission = info['fee']
        timestamp = datetime.fromtimestamp(info['time'])
        return OrderExecutionInfo(price, quantity, commission, timestamp)

    @lazy_init
    def OrderExecutionInfo(self, since):
        """ return a dict of recent trades as OrderExecutionInfo objects
        """
        def fetch(offset):
            return self.trades_history(offset, ttype='all', trades=True, start=since)

        return { txid: self._trade_to_OEI(info, txid) for txid, info in
                self._fetch_all(fetch, 'trades').items() }

    def ClosedOrders(self, since, symbols=None):
        """Return a list of filled Orders newer than <since>
        since is a unix timestamp
        symbol is a list of symbols, or None for all
        """
        def get_oei_info():
            return self.OrderExecutionInfo(since)

        def fetch(offset):
            return self.closed_orders(offset, trades=True, start=since)

        sym_check = lambda o: SYMBOL_LOCAL[o['descr']['pair']] in symbols
        all_wanted = lambda o: True

        wanted = sym_check if symbols else all_wanted

        return [self._order_to_Order(txid, oinfo, get_oei_info) for txid, oinfo in
                self._fetch_all(fetch, 'closed').items() if wanted(oinfo)]








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


