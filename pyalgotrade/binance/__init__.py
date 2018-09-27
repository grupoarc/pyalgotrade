from pyalgotrade import Symbol

# venue name
VENUE = 'binance'

# Default symbol to trade; shouldn't be used much
DEFAULT_SYMBOL = Symbol.BNBBTC

# All supported symbols
SYMBOLS = [ Symbol.BNBBTC, Symbol.BNBETH ]

# map from common pyalgotrade symbol to local symbols
LOCAL_SYMBOL = { s: str(s) for s in SYMBOLS  }
# reverse of above
SYMBOL_LOCAL = { v: k for k, v in LOCAL_SYMBOL.items() }


def cc1cc2(sym):
    """"Binance only quotes in BTC, ETH, USDT, and BNB.  Figure out which this is.
        Accepts a pyalgotrade symbol.
    """
    ssym = str(sym)
    for cc in ('BTC', 'ETH', 'USDT', 'BNB'):
        if ssym.endswith(cc):
            return ssym[:-len(cc)], cc
    raise ValueError("Not a valid Binance Symbol: {!s}".format(sym))


