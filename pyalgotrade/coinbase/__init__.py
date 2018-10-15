from pyalgotrade import Symbol

VENUE = 'coinbase'

LOCAL_SYMBOL = {Symbol.BTC_USD: 'BTC-USD',
                Symbol.BTC_EUR: 'BTC-EUR'
                }

SYMBOL_LOCAL = { v: k for k, v in LOCAL_SYMBOL.items() }
SYMBOLS = list(LOCAL_SYMBOL.keys())
