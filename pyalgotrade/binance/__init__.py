from pyalgotrade import Symbol

# venue name
VENUE = 'binance'

# Default symbol to trade; shouldn't be used much
DEFAULT_SYMBOL = Symbol.BNBBTC

# All supported symbols
SYMBOLS = [ Symbol.BNBBTC, Symbol.BNBETH ]

# map from common pyalgotrade symbol to local symbols
LOCAL_SYMBOL = { s: str(s).replace('-','') for s in SYMBOLS  }
# reverse of above
SYMBOL_LOCAL = { v: k for k, v in LOCAL_SYMBOL.items() }

