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
'_18C',
'AAC',
'ABT',
'ACT',
'ADA',   # Cardano
'ADX',   # AdEx
'AE',    # Aeternity
'AGI',   # SingularityNET
'AIDOC',
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
'BCV',
'BCX',
'BCPT',
'BFT',
'BIFI',
'BIX',
'BKBT',
'BLZ',
'BNB',   # Binance Bucks
'BNT',
'BOX',
'BQX',
'BRD',
'BT1',
'BT2',
'BTC',   # Bitcoin
'BTG',
'BTM',
'BTS',
'BUT',
'CDC',
'CDT',
'CHAT',
'CLOAK',
'CMT',
'CND',
'CNN',
'CTXC',
'CVC',
'CVCOIN',
'DAC',
'DASH',  # Dash
'DAT',
'DATA',
'DATX',
'DBC',
'DCR',
'DENT',
'DGB',
'DGD',
'DLT',
'DNT',
'DOCK',
'DTA',
'EDO',
'EDU',
'EGCC',
'EKO',
'EKT',
'ELA',
'ELF',
'ENG',
'ENJ',
'EOS',   # EOS
'ETC',   # Ethereum Classic
'ETH',   # Ethereum
'EVX',
'FAIR',
'FUEL',
'FTI',
'FUN',
'GAS',
'GET',
'GNO',   # Gnosis
'GNT',
'GNX',
'GO',
'GRS',
'GSC',
'GTC',
'GTO',
'GVE',
'GVT',
'GXS',
'HB10',
'HC',
'HIT',
'HOT',
'HPT',
'HSR',
'HT',
'ICN',   # Iconomi
'ICX',
'IDT',
'IIC',
'INS',
'IOST',
'IOTA',
'IOTX',
'ITC',
'KAN',
'KCASH',
'KEY',
'KMD',
'KNC',
'LBA',
'LEND',
'LET',
'LINK',
'LOOM',
'LRC',
'LSK',
'LTC',   # Litecoin
'LUN',
'LXT',
'LYM',
'MAN',
'MANA',
'MCO',
'MDA',
'MDS',
'MEET',
'MEX',
'MFT',
'MLN',   # Melon
'MOD',
'MTH',
'MT',
'MTL',
'MTN',
'MTX',
'MUSK',
'NANO',
'NAS',
'NAV',
'NCASH',
'NCC',
'NEBL',
'NEO',
'NPXS',
'NULS',
'NXS',
'OAX',
'OCN',
'OMG',
'ONT',
'OST',
'PAI',
'PAX',
'PAY',
'PC',
'PHX',
'PIVX',
'PNT',
'POA',
'POE',
'POLY',
'PORTAL',
'POWR',
'PPT',
'PROPY',
'QASH',
'QKC',
'QLC',
'QSP',
'QTUM',
'QUN',
'RCCC',
'RCN',
'RDN',
'REN',
'REP',   # Augur REP tokens
'REQ',
'RLC',
'RPX',
'RTE',
'RUFF',
'SALT',
'SBTC',
'SC',
'SEELE',
'SHE',
'SKY',
'SMT',
'SNC',
'SNGLS',
'SNM',
'SNT',
'SOC',
'SRN',
'SSP',
'STEEM',
'STK',
'STORJ',
'STORM',
'STRAT',
'SUB',
'SWFTC',
'SYS',
'THETA',
'TNB',
'TNT',
'TOPC',
'TOS',
'TRIG',
'TRIO',
'TRX',
'TUSD',
'UC',
'UIP',
'USDT',  # USD-Tether
'UTK',
'UUU',
'VEN',
'VET',
'VIA',
'VIB',
'VIBE',
'WABI',
'WAN',
'WAVES',
'WAX',
'WICC',
'WINGS',
'WPR',
'WTC',
'XDG',   # Dogecoin
'XEM',
'XLM',   # Stellar/Lumens
'XMR',   # Monero
'XMX',
'XRP',   # Ripple
'XVG',
'XZC',
'YCC',
'YEE',
'YOYO',
'ZEC',   # Z-cash
'ZEN',
'ZIL',
'ZJLT',
'ZLA',
'ZRX',
]

CCYs = FIAT_CCYs + CRYPTO_CCYs

CC2s = FIAT_CCYs + [ 'BNB', 'BTC', 'ETH', 'USDT', 'HT', 'HUSD' ]

Symbol = Enum('Symbol', [(c, c) for c in CCYs] + [(c1+"_"+c2, c1+"/"+c2) for c1 in CCYs for c2 in CC2s if c1 != c2])

Symbol.__doc__ = """Pyalgotrade standard symbol enum.  Symbols are strings of the form CCY1-CCY2."""

Symbol.__str__ = lambda self: self.name

Symbol.cc1cc2 =  lambda self: tuple(([Symbol[c] for c in str(self).split('-')] + [None])[:2])

