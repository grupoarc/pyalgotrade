import os
import time
from decimal import Decimal

import pytest
import simplejson as json

from pyalgotrade.orderbook import OrderBook, MarketUpdate, MarketSnapshot
from pyalgotrade.coinbase.netclients import toBookMessages

def to_json(o):
    return json.dumps(o, sort_keys=True, indent=4)

def fixup_decimal(d):
    d = Decimal(d)
    return d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()

def fixup_float(f):
    if float(f) - int(f) == 0: return int(f)
    return round(float(f), 12)

fixup = fixup_decimal

def normalize_side(side):
    newside = [ [fixup(pl.price), fixup(pl.size)] for pl in side ]
    return newside

@pytest.mark.parametrize("sessionfilename", [
    "replay_coinbase_0",
    "replay_coinbase_1",
    "replay_coinbase_2"
])
def test_replay_coinbase_session(sessionfilename):

    book = OrderBook('testvenue', 'testsymbol')

    infilename = os.path.join(os.path.dirname(__file__), sessionfilename)

    now = time.time()
    book.update(MarketSnapshot(now, 'testvenue', 'testsymbol', [])) # reset the book
    with open(infilename + ".in") as infile:
        for line in infile:
            date, hms, jmsg = line.split(' ', 2)
            book.update(MarketUpdate(now, 'testvenue', 'testsymbol', toBookMessages(jmsg, 'testsymbol')))

    expected = {}
    with open(infilename + ".out") as infile:
        expected_s = ''.join(infile.readlines())
        expected = json.loads(expected_s)

    result = book.nvolume(len(expected["asks"]))

    assert len(expected['asks']) == len(result['asks'])
    assert to_json(expected['asks']) == to_json(normalize_side(result['asks']))
    assert len(expected['bids']) == len(result['bids'])
    assert to_json(expected['bids']) == to_json(normalize_side(result['bids']))

