"""Timing only, after original owner receipt identity and source checks.

Binance's scheduled close timestamp is not a claim of exchange-side immutability.
No transport, proof minting, revisions, or changes to acceptance authority.
"""
from dynamic_minute_span import valid_minute_span
MINUTE=60_000

def admitted_closed_clock(end,expires,request,received,now,rows):
    if any(type(t) is not int for t in (end,expires,request,received,now)):
        return False
    if end<=0 or end%(15*MINUTE) or not end<=request<=received<=now<expires<=end+MINUTE:
        return False
    if type(rows) is not tuple or not valid_minute_span(len(rows)):
        return False
    for t,r in zip(range(end-len(rows)*MINUTE,end,MINUTE),rows,strict=True):
        if (type(r) is not tuple or len(r)!=12 or type(r[0]) is not int
                or type(r[6]) is not int or r[0]!=t or r[6]!=t+MINUTE-1 or r[6]>=request):
            return False
    return True
