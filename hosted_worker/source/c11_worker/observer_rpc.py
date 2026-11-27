"""Bounded, read-only observer facts on the same trusted child pipe as OpenMarket."""
from exact_symbol import ID_CHAR
from decimal import Decimal
import re
import secrets
import time

from bridge_protocol import need

OP = 'READ_C11_OBSERVER_V1'
FIVE = 300_000
DAY = 86_400_000
SYMBOL = re.compile(f'{ID_CHAR}{{2,40}}')
HEX = re.compile('[a-f0-9]{64}')


def now_ms():
    return time.time_ns() // 1_000_000


def clock(value):
    return type(value) is int and value > 0


class ObserverClient:
    def __init__(self,pipe,binding):
        need(type(binding) is str and HEX.fullmatch(binding),'C11_OBSERVER_ATTESTED_BINDING_REQUIRED')
        self.pipe,self.binding=pipe,binding

    def _read(self,view,symbol,end,start=None):
        need(type(symbol) is str and SYMBOL.fullmatch(symbol) and clock(end),
             'C11_OBSERVER_REQUEST_SHAPE')
        nonce=secrets.token_hex(16)
        request=dict(op=OP,nonce=nonce,view=view,symbol=symbol,end_ms=end,start_ms=start)
        self.pipe.send(request)
        reply=self.pipe.receive(2)
        need(type(reply) is dict and reply.get('nonce')==nonce and reply.get('binding')==self.binding
             and reply.get('view')==view and reply.get('symbol')==symbol
             and reply.get('end_ms')==end and reply.get('start_ms')==start,'C11_OBSERVER_REPLY_IDENTITY')
        return reply.get('data')

    def point(self,symbol,decision):
        data=self._read('point',symbol,decision)
        if data is None:
            return None
        generation,rank,raw=data
        ratio=Decimal(raw)
        need(type(generation) is int and generation>0 and type(rank) is int and 1<=rank<=60
             and ratio.is_finite(),'C11_OBSERVER_POINT_VALUE')
        return generation,rank,ratio

    def weak(self,symbol,decision):
        data=self._read('weak',symbol,decision)
        if data is None:
            return None
        need(decision%FIVE==0 and [row[0] for row in data]==[decision-7*FIVE,decision-FIVE],
             'C11_OBSERVER_WEAK_CLOCK')
        values=[]
        for bound,raw,seen in data:
            quantity=Decimal(raw)
            need(quantity.is_finite() and quantity>0 and type(seen) is int
                 and bound<=seen<bound+FIVE and seen<=decision,'C11_OBSERVER_WEAK_VALUE')
            values.append((bound,quantity,seen))
        return tuple(values)

    def span(self,symbol,start,end):
        need(clock(start) and start<=end and end-start<=DAY,'C11_OBSERVER_SPAN_SCOPE')
        data=self._read('span',symbol,end,start)
        if data is not None:
            need(len(data)==2 and all(type(g) is int and g>0 for g in data)
                 and data[0]<=data[1],'C11_OBSERVER_SPAN_VALUE')
        return tuple(data) if data is not None else None

    def episode(self,symbol,end):
        data=self._read('episode',symbol,end)
        if data is not None:
            need(clock(data) and data%FIVE==0 and data<=end and end-data<=DAY,
                 'C11_OBSERVER_EPISODE_VALUE')
        return data
