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
'ADA',   # Cardano
'ADX',   # AdEx
'AE',    # Aeternity
'AGI',   # SingularityNET
'AION',
'AMB',
'APPC',
'ARDR',
'ARK',
'ARN',
'AST',
'BAT',
'BCD',
'BCH',   # Bitcoin Cash
'BCN',
'BCPT',
'BLZ',
'BNB',   # Binance Bucks
'BNT',
'BQX',
'BRD',
'BTC',   # Bitcoin
'BTG',
'BTS',
'CDT',
'CHAT',
'CLOAK',
'CMT',
'CND',
'CVC',
'DASH',  # Dash
'DATA',
'DENT',
'DGD',
'DLT',
'DNT',
'DOCK',
'EDO',
'ELF',
'ENG',
'ENJ',
'EOS',   # EOS
'ETC',   # Ethereum Classic
'ETH',   # Ethereum
'EVX',
'FUEL',
'FUN',
'GAS',
'GNO',   # Gnosis
'GNT',
'GO',
'GRS',
'GTO',
'GVT',
'GXS',
'HC',
'HOT',
'HSR',
'ICN',   # Iconomi
'ICX',
'INS',
'IOST',
'IOTA',
'IOTX',
'KEY',
'KMD',
'KNC',
'LEND',
'LINK',
'LOOM',
'LRC',
'LSK',
'LTC',   # Litecoin
'LUN',
'MANA',
'MCO',
'MDA',
'MFT',
'MLN',   # Melon
'MOD',
'MTH',
'MTL',
'NANO',
'NAS',
'NAV',
'NCASH',
'NEBL',
'NEO',
'NPXS',
'NULS',
'NXS',
'OAX',
'OMG',
'ONT',
'OST',
'PAX',
'PHX',
'PIVX',
'POA',
'POE',
'POLY',
'POWR',
'PPT',
'QKC',
'QLC',
'QSP',
'QTUM',
'RCN',
'RDN',
'REP',   # Augur REP tokens
'REQ',
'RLC',
'RPX',
'SALT',
'SC',
'SKY',
'SNGLS',
'SNM',
'SNT',
'STEEM',
'STORJ',
'STORM',
'STRAT',
'SUB',
'SYS',
'THETA',
'TNB',
'TNT',
'TRIG',
'TRX',
'TUSD',
'USDT',  # USD-Tether
'VEN',
'VET',
'VIA',
'VIB',
'VIBE',
'WABI',
'WAN',
'WAVES',
'WINGS',
'WPR',
'WTC',
'XDG',   # Dogecoin
'XEM',
'XLM',   # Stellar/Lumens
'XMR',   # Monero
'XRP',   # Ripple
'XVG',
'XZC',
'YOYO',
'ZEC',   # Z-cash
'ZEN',
'ZIL',
'ZRX',
]

CCYs = FIAT_CCYs + CRYPTO_CCYs

CC2s = FIAT_CCYs + [ 'BNB', 'BTC', 'ETH', 'USDT' ]

Symbol = Enum('Symbol', [(c, c) for c in CCYs] + [(c1+"_"+c2, c1+"/"+c2) for c1 in CCYs for c2 in CC2s if c1 != c2])

Symbol.__doc__ = """Pyalgotrade standard symbol enum.  Symbols are strings of the form CCY1-CCY2."""

Symbol.__str__ = lambda self: self.name

Symbol.cc1cc2 =  lambda self: tuple(([Symbol[c] for c in str(self).split('-')] + [None])[:2])

