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
  "BCHEUR": Symbol.BCH_EUR,
  "BCHUSD": Symbol.BCH_USD,
  "BCHXBT": Symbol.BCH_BTC,
  "DASHEUR": Symbol.DASH_EUR,
  "DASHUSD": Symbol.DASH_USD,
  "DASHXBT": Symbol.DASH_BTC,
  "EOSETH": Symbol.EOS_ETH,
  "EOSEUR": Symbol.EOS_EUR,
  "EOSUSD": Symbol.EOS_USD,
  "EOSXBT": Symbol.EOS_BTC,
  "GNOETH": Symbol.GNO_ETH,
  "GNOEUR": Symbol.GNO_EUR,
  "GNOUSD": Symbol.GNO_USD,
  "GNOXBT": Symbol.GNO_BTC,
  "USDTZUSD": Symbol.USDT_USD,
  "XETCXETH": Symbol.ETC_ETH,
  "XETCXXBT": Symbol.ETC_BTC,
  "XETCZEUR": Symbol.ETC_EUR,
  "XETCZUSD": Symbol.ETC_USD,
  "XETHXXBT": Symbol.ETH_BTC,
  "XETHZCAD": Symbol.ETH_CAD,
  "XETHZEUR": Symbol.ETH_EUR,
  "XETHZGBP": Symbol.ETH_GBP,
  "XETHZJPY": Symbol.ETH_JPY,
  "XETHZUSD": Symbol.ETH_USD,
  "XICNXETH": Symbol.ICN_ETH,
  "XICNXXBT": Symbol.ICN_BTC,
  "XLTCXXBT": Symbol.LTC_BTC,
  "XLTCZEUR": Symbol.LTC_EUR,
  "XLTCZUSD": Symbol.LTC_USD,
  "XMLNXETH": Symbol.MLN_ETH,
  "XMLNXXBT": Symbol.MLN_BTC,
  "XREPXETH": Symbol.REP_ETH,
  "XREPXXBT": Symbol.REP_BTC,
  "XREPZEUR": Symbol.REP_EUR,
  "XREPZUSD": Symbol.REP_USD,
  "XXBTZCAD": Symbol.BTC_CAD,
  "XXBTZEUR": Symbol.BTC_EUR,
  "XXBTZGBP": Symbol.BTC_GBP,
  "XXBTZJPY": Symbol.BTC_JPY,
  "XXBTZUSD": Symbol.BTC_USD,
  "XXDGXXBT": Symbol.XDG_BTC,
  "XXLMXXBT": Symbol.XLM_BTC,
  "XXLMZEUR": Symbol.XLM_EUR,
  "XXLMZUSD": Symbol.XLM_USD,
  "XXMRXXBT": Symbol.XMR_BTC,
  "XXMRZEUR": Symbol.XMR_EUR,
  "XXMRZUSD": Symbol.XMR_USD,
  "XXRPXXBT": Symbol.XRP_BTC,
  "XXRPZCAD": Symbol.XRP_CAD,
  "XXRPZEUR": Symbol.XRP_EUR,
  "XXRPZJPY": Symbol.XRP_JPY,
  "XXRPZUSD": Symbol.XRP_USD,
  "XZECXXBT": Symbol.ZEC_BTC,
  "XZECZEUR": Symbol.ZEC_EUR,
  "XZECZJPY": Symbol.ZEC_JPY,
  "XZECZUSD": Symbol.ZEC_USD,
  "ZUSD": Symbol.USD,
  "ZEUR": Symbol.EUR,
  "ZCAD": Symbol.CAD,
  "ZJPY": Symbol.JPY,
  "ZGBP": Symbol.GBP
})

# map from common pyalgotrade symbol to local symbols
LOCAL_SYMBOL = { v: k for k, v in SYMBOL_LOCAL.items() }

# supported symbols
SYMBOLS = list(LOCAL_SYMBOL.keys())

# extra aliases
SYMBOL_LOCAL.update({
    "XBTUSD": Symbol.BTC_USD,
    "XBTCAD": Symbol.BTC_CAD,
    "XBTEUR": Symbol.BTC_EUR,
    "XBTGBP": Symbol.BTC_GBP,
    "XBTJPY": Symbol.BTC_JPY,
    "XBTUSD": Symbol.BTC_USD,
})

