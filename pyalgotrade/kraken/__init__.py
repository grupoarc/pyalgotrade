from pyalgotrade import Symbol

VENUE = 'kraken'

def build_symbols():
    # these are all the same
    syms = ["EUR", "USD",
            "BCH", "DASH", "EOS", "ETC", "ETH", "GNO", "ICN", "LTC", "MLN", "REP", "USDT", "XLM", "XMR", "XRP", "ZEC"
           ]
    ret = { s: Symbol[s] for s in syms }
    # bitcoin is different
    ret['XBT'] = Symbol.BTC
    ret['XXBT'] = Symbol.BTC
    return ret

SYMBOL_LOCAL = build_symbols()

# map from kraken symbols to common pyalgotrade symbols
# list from
# wget https://api.kraken.com/0/public/AssetPairs -qO - | python -c "import sys, json; l=json.load(sys.stdin)['result']; print(json.dumps(dict((k,'Symbol.'+str(k)) for k,v in l.items()),sort_keys=True,indent=2))"
# then hand-edit in the symbols
SYMBOL_LOCAL.update({
  "BCHEUR": Symbol.BCHEUR,
  "BCHUSD": Symbol.BCHUSD,
  "BCHXBT": Symbol.BCHBTC,
  "DASHEUR": Symbol.DASHEUR,
  "DASHUSD": Symbol.DASHUSD,
  "DASHXBT": Symbol.DASHBTC,
  "EOSETH": Symbol.EOSETH,
  "EOSEUR": Symbol.EOSEUR,
  "EOSUSD": Symbol.EOSUSD,
  "EOSXBT": Symbol.EOSBTC,
  "GNOETH": Symbol.GNOETH,
  "GNOEUR": Symbol.GNOEUR,
  "GNOUSD": Symbol.GNOUSD,
  "GNOXBT": Symbol.GNOBTC,
  "USDTZUSD": Symbol.USDTUSD,
  "XETCXETH": Symbol.ETCETH,
  "XETCXXBT": Symbol.ETCBTC,
  "XETCZEUR": Symbol.ETCEUR,
  "XETCZUSD": Symbol.ETCUSD,
  "XETHXXBT": Symbol.ETHBTC,
  "XETHZCAD": Symbol.ETHCAD,
  "XETHZEUR": Symbol.ETHEUR,
  "XETHZGBP": Symbol.ETHGBP,
  "XETHZJPY": Symbol.ETHJPY,
  "XETHZUSD": Symbol.ETHUSD,
  "XICNXETH": Symbol.ICNETH,
  "XICNXXBT": Symbol.ICNBTC,
  "XLTCXXBT": Symbol.LTCBTC,
  "XLTCZEUR": Symbol.LTCEUR,
  "XLTCZUSD": Symbol.LTCUSD,
  "XMLNXETH": Symbol.MLNETH,
  "XMLNXXBT": Symbol.MLNBTC,
  "XREPXETH": Symbol.REPETH,
  "XREPXXBT": Symbol.REPBTC,
  "XREPZEUR": Symbol.REPEUR,
  "XREPZUSD": Symbol.REPUSD,
  "XXBTZCAD": Symbol.BTCCAD,
  "XXBTZEUR": Symbol.BTCEUR,
  "XXBTZGBP": Symbol.BTCGBP,
  "XXBTZJPY": Symbol.BTCJPY,
  "XXBTZUSD": Symbol.BTCUSD,
  "XXDGXXBT": Symbol.XDGBTC,
  "XXLMXXBT": Symbol.XLMBTC,
  "XXLMZEUR": Symbol.XLMEUR,
  "XXLMZUSD": Symbol.XLMUSD,
  "XXMRXXBT": Symbol.XMRBTC,
  "XXMRZEUR": Symbol.XMREUR,
  "XXMRZUSD": Symbol.XMRUSD,
  "XXRPXXBT": Symbol.XRPBTC,
  "XXRPZCAD": Symbol.XRPCAD,
  "XXRPZEUR": Symbol.XRPEUR,
  "XXRPZJPY": Symbol.XRPJPY,
  "XXRPZUSD": Symbol.XRPUSD,
  "XZECXXBT": Symbol.ZECBTC,
  "XZECZEUR": Symbol.ZECEUR,
  "XZECZJPY": Symbol.ZECJPY,
  "XZECZUSD": Symbol.ZECUSD,
})

# map from common pyalgotrade symbol to local symbols
LOCAL_SYMBOL = { v: k for k, v in SYMBOL_LOCAL.items() }

# supported symbols
SYMBOLS = list(LOCAL_SYMBOL.keys())

