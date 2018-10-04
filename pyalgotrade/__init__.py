# PyAlgoTrade
#
# Copyright 2011-2018 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

name = "PyAlgoTrade"
__version__ = "0.20"

from enum import Enum

FIAT_CCYs = [ 'CAD', 'EUR', 'GBP', 'JPY', 'USD' ]

CRYPTO_CCYs = [
'BCH',   # Bitcoin Cash
'BNB',   # Binance Bucks
'BTC',   # Bitcoin
'DASH',  # Dash
'EOS',   # EOS
'ETC',   # Ethereum Classic
'ETH',   # Ethereum
'GNO',   # Gnosis
'ICN',   # Iconomi
'LTC',   # Litecoin
'MLN',   # Melon
'REP',   # Augur REP tokens
'USDT',  # USD-Tether
'XDG',   # Dogecoin
'XLM',   # Stellar/Lumens
'XMR',   # Monero
'XRP',   # Ripple
'ZEC',   # Z-cash
]

CCYs = FIAT_CCYs + CRYPTO_CCYs

Symbol = Enum('Symbol', [(c, c) for c in CCYs] + [(c1+c2, c1+"-"+c2) for c1 in CCYs for c2 in CCYs if c1 != c2])

Symbol.__doc__ = """Pyalgotrade standard symbol enum.  Symbols are strings of the form CCY1-CCY2."""

Symbol.__str__ = lambda self: self.name

Symbol.cc1cc2 =  lambda self: tuple(([Symbol[c] for c in str(self).split('-')] + [None])[:2])
