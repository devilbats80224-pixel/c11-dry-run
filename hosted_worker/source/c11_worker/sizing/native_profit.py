"""Exact native long-futures calculation order using Decimal string products.
Native FtPrecise computes each open/close notional+fee precisely, converts each
to float, adds funding to close value, subtracts open value, then round(...,8).
Called only on closed trades to keep future sizing on native realized-wallet path.
"""
from decimal import Decimal

def realized_profit(qty,entry,exit,funding):
 q,p0,p1=map(lambda x:Decimal(str(x)),(qty,entry,exit));fee=Decimal('0.0005')
 opened=float(q*p0+q*p0*fee)
 closed=float(q*p1-q*p1*fee)+funding
 return round(closed-opened,8)
