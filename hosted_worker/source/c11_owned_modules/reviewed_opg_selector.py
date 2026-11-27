"""Pure-paper, exact OPG revision provider. Startup digest is disabled by default."""
from hashlib import sha256
from pathlib import Path
from threading import get_ident
import json, os, re, stat
from bridge_protocol import Refused
from binding import load
from causal_bars import FIVE, QUARTER, MINUTE
from atr_er_checkpoint import AtrErCheckpoint
from rpd_checkpoint import RpdState
from multilane_history_view import AuditedKey, FrozenReadView, enc, digest, rowpin
from prospective_derivation import Prior, parse_prior, restore_memory

SYMBOL='OPGUSDT'
PAIR='OPG/USDT:USDT'
KEYS=((MINUTE,1795581840000),(FIVE,1795581600000),(QUARTER,1795581000000))
HEX=re.compile('[0-9a-f]{64}')
EVENT=re.compile('Sentinel_[0-9a-f]{32}')
MAX_CLOSES=96*7
TABLE="opg_corrected_lineage_v1"
DIR=Path('/opt/c11-owned-revision')
REQUIRED={'schema','owner_event','owner_at_ms','runtime_sha','prior_sha',
          'independent_reproduction_sha','reviewed_available_ms','use_until_ms'}

def require(ok, label):
    if not ok:
        raise Refused(label)

def read_pinned(path, pin, max_bytes):
    require(type(pin) is str and HEX.fullmatch(pin),'OPG_REVISION_DIGEST_REQUIRED')
    with os.fdopen(os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK),'rb') as f:
        s=os.fstat(f.fileno())
        require(stat.S_ISREG(s.st_mode) and (s.st_uid,s.st_gid,s.st_nlink)==(0,0,1)
           and stat.S_IMODE(s.st_mode)==0o444 and 0<s.st_size<=max_bytes
           and os.fstatvfs(f.fileno()).f_flag&os.ST_RDONLY,'OPG_REVISION_IMMUTABLE_SOURCE')
        raw=f.read(max_bytes+1)
    require(len(raw)==s.st_size and sha256(raw).hexdigest()==pin,'OPG_REVISION_PIN_MISMATCH')
    return raw

class ReviewedOpgSource:
    def __init__(self, manifest, prior, keys):
        self.manifest=manifest
        self.prior=prior
        self.keys=keys

    def bind(self, bot, plan, store, adapter, wall, permitted):
        return OpgSelector(self,bot,plan,store,adapter,wall,permitted)

def installed_review(pin,runtime,now):
    if pin is None:
        return None
    envelope=load(read_pinned(DIR/'review.json',pin,8192),8192)
    require(type(envelope) is dict and set(envelope)==REQUIRED
       and envelope['schema']=='C11_OPG_1245_HISTORY_ONLY_V1'
       and envelope['runtime_sha']==runtime and type(envelope['owner_event']) is str
       and EVENT.fullmatch(envelope['owner_event'])
       and all(type(envelope[k]) is int for k in ('owner_at_ms','reviewed_available_ms','use_until_ms'))
       and envelope['owner_at_ms']<=envelope['reviewed_available_ms']<=now<envelope['use_until_ms'],
       'OPG_REVISION_REVIEW_INVALID')
    prior_pin=envelope['prior_sha']
    raw=read_pinned(DIR/'prior.json',prior_pin,131072)
    reproduction=read_pinned(DIR/'reproduction.json',envelope['independent_reproduction_sha'],8192)
    side=load(reproduction,8192)
    require(type(side) is dict and set(side)=={'schema','prior_sha','completed_ms','state_sha','scope'}
       and side['schema']=='C11_OPG_INDEPENDENT_CORRECTED_CARRIER_V1'
       and side['prior_sha']==prior_pin and side['scope']==SYMBOL
       and type(side['completed_ms']) is int and side['completed_ms']<=envelope['reviewed_available_ms'],
       'OPG_REVISION_REPRODUCTION_INVALID')
    packet=load(raw,131072)
    require(digest(enc(packet['derived_state']))==side['state_sha'],'OPG_REVISION_REPRODUCTION_STATE')
    prior=parse_prior(raw,prior_pin,independent_readback=lambda source,h:source is raw and h==prior_pin)
    require(prior.symbol==SYMBOL and prior.observed<=side['completed_ms'],'OPG_REVISION_CARRIER_ID')
    keys=tuple(sorted((AuditedKey(x['width'],x['open_ms'],x['old_seen_ms'],x['new_seen_ms'],
                        x['prior_sha'],x['revised_sha']) for x in packet['per_width_exact_conflict_evidence']),
                      key=lambda k:k.width))
    require(tuple((k.width,k.open_ms) for k in keys)==KEYS and all(
        type(k.old_seen) is type(k.new_seen) is int and k.open_ms+k.width<=k.old_seen<k.new_seen<=prior.observed
        and HEX.fullmatch(k.previous_pin) and HEX.fullmatch(k.revised_pin) and k.previous_pin!=k.revised_pin
        for k in keys),'OPG_REVISION_KEYS_CHANGED')
    return ReviewedOpgSource(envelope,prior,keys)

class OpgSelector:
    def __init__(self, source, bot, plan, store, adapter, wall, permitted):
        self.source,self.bot,self.plan,self.store,self.adapter=source,bot,plan,store,adapter
        self.wall,self.permitted,self.owner=wall,permitted,get_ident()
        self._pending=None

    def _table_exists(self):
        return self.store.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(TABLE,)).fetchone() is not None

    def _prior(self, end, before):
        base=self.source.prior
        if not self._table_exists():return base
        rows=self.store.db.execute(f'''SELECT cursor,parent_sha,carrier_sha,bytes,known_ms,receipt_sha,proof_sha,gen
             FROM {TABLE} WHERE anchor=? AND cursor<? ORDER BY cursor LIMIT ?''',
             (base.pin,end,MAX_CLOSES+1)).fetchall()
        require(len(rows)<=MAX_CLOSES,'OPG_REVISION_RECHECKPOINT_REQUIRED')
        for cursor,parent,carrier,blob,known,receipt,proof,gen in rows:
            a=load(blob,131072)
            delta=cursor-base.cursor
            kind=a.get('kind')
            forward=(delta==QUARTER if kind is None else
                     kind=='DATA_ONLY_RECOVERY_V1' and delta%QUARTER==0 and 0<delta<=4*QUARTER
                     and type(known) is int and known>=cursor+65000)
            require(parent==base.pin and forward and sha256(blob).hexdigest()==carrier
                    and type(known) is int and cursor<=known<before and type(gen) is int and gen>0
                    and HEX.fullmatch(receipt) and proof==digest(enc((parent,carrier,cursor,known,receipt,gen))),
                    'OPG_REVISION_CORRECTED_CHAIN_INVALID')
            require((a['symbol'],a['origin_ms'],a['cursor_ms'],a['recomputation_at_ms'])==
                    (SYMBOL,base.origin,cursor,known),'OPG_REVISION_CORRECTED_CHAIN_ID')
            base=Prior(SYMBOL,base.origin,cursor,known,carrier,blob)
        return base

    def append_settled_data(self, base, stop, generation, receipt_pin, lanes, due):
        # No Scope, callback, original candle/checkpoint updates, or replayed
        # eligibility. At most four already settled quarters delivered by the
        # existing original-SDK history child, with owner lease independently checked.
        start=self.wall()
        require(get_ident()==self.owner and stop>=base.cursor+QUARTER
                and stop-base.cursor<=4*QUARTER and stop+65000<=start and due() is True
                and HEX.fullmatch(receipt_pin),'OPG_RECOVERY_OUTSIDE_ORIGINAL_PHASE')
        mem,atr,segment=restore_memory(base)
        try:
            for w in (FIVE,QUARTER):
                tail=lanes[w]
                require(tuple(q.open_ms for q in tail)==tuple(range(base.cursor,stop,w))
                        and all(stop<=q.received_ms<=start for q in tail),'OPG_RECOVERY_CARRY_INTERVAL')
                mem.record(SYMBOL,w,tail)
                require(atr.extend(SYMBOL,w,segment,stop,start)==stop,'OPG_RECOVERY_ATR_CURSOR')
            state=mem.rpd_progress(SYMBOL,stop,start)
            require(state.next_open==stop and state.closed().known is True,'OPG_RECOVERY_RPD_CURSOR')
            known=self.wall()
            carrier=enc(dict(kind='DATA_ONLY_RECOVERY_V1',symbol=SYMBOL,origin_ms=base.origin,
                cursor_ms=stop,recomputation_at_ms=known,derived_state=dict(rpd_state=json.loads(state.pack()),
                    atr={str(w):dict(state=atr._state(SYMBOL,w,segment)[2],
                        latest=atr.sample(SYMBOL,w,segment,stop,start),
                        prior=atr.sample(SYMBOL,w,segment,stop-w,start)) for w in (FIVE,QUARTER)})))
        finally:
            mem.db.close()
        db=self.store.db
        require(not db.in_transaction and due() is True,'OPG_RECOVERY_OWNER_REVOKED')
        with db:
            db.execute('BEGIN IMMEDIATE')
            require(due() is True and self._prior(stop+QUARTER,known).pin==base.pin,
                    'OPG_RECOVERY_PARENT_CHANGED')
            self._append_record(base,stop,generation,receipt_pin,carrier,known)
            require(due() is True,'OPG_RECOVERY_OWNER_REVOKED')
        return True

    def append(self, store, scope, view):
        pending=self._pending; self._pending=None
        require(pending is not None and get_ident()==self.owner and store is self.store
                and store.db.in_transaction and view is pending[0] and view.bound(scope,self.wall())
                and self.plan.current(scope,self.wall()) is True and self.permitted(scope,self.wall()) is True,
                'OPG_REVISION_PRIVATE_APPEND_AUTHORITY')
        _,base,blob,known=pending
        self._append_record(base,scope.end,scope.generation,view.current_receipt_pin,blob,known)
        return True

    def _append_record(self,base,cursor,generation,receipt_pin,blob,known):
        db=self.store.db; anchor=self.source.prior.pin; carrier=sha256(blob).hexdigest()
        require(db.in_transaction,'OPG_RECOVERY_SOLE_NATIVE_TRANSACTION')
        proof=digest(enc((base.pin,carrier,cursor,known,receipt_pin,generation)))
        db.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE} (anchor TEXT NOT NULL,cursor INTEGER NOT NULL,
            parent_sha TEXT NOT NULL,carrier_sha TEXT NOT NULL,bytes BLOB NOT NULL,known_ms INTEGER NOT NULL,
            receipt_sha TEXT NOT NULL,proof_sha TEXT NOT NULL,gen INTEGER NOT NULL,PRIMARY KEY(anchor,cursor))''')
        # Never REPLACE/UPDATE: duplicate or changed generation for this cursor is
        # refused, not a second derived-state commit. The native transaction owns rollback.
        require(db.execute(f'SELECT 1 FROM {TABLE} WHERE anchor=? AND cursor=?',(anchor,cursor)).fetchone() is None,
                'OPG_REVISION_PRIVATE_CURSOR_ALREADY_COMMITTED')
        db.execute(f'INSERT INTO {TABLE} VALUES(?,?,?,?,?,?,?,?,?)',
                   (anchor,cursor,base.pin,carrier,blob,known,receipt_pin,proof,generation))

    @staticmethod
    def _row_match(group, raw, rx):
        return len(group)==len(raw) and all(
              (q.open_ms,q.received_ms,q.op,q.high,q.low,q.close,q.volume,q.quote)
              ==(r[0],rx,*(float(r[i]) for i in (1,2,3,4,5,7)))
              for q,r in zip(group,raw,strict=True))

    def claims(self, s):
        # Even stale/missing carry for this exact exceptional route must not fall
        # through to a canonical judgment. Scope and phase are checked below.
        return (s.pair,s.symbol)==(PAIR,SYMBOL)

    def __call__(self, s, batches, adapter):
        return self._prepare(s,batches,adapter,None)

    def from_reconfirm(self,s,batches,actor):
        from reconfirm_input import OriginalReconfirm,Final
        require(type(actor) is OriginalReconfirm and actor.adapter is self.adapter,
                'OPG_REVISION_WRONG_RECONFIRM_ACTOR')
        chain=actor.scoped_revision_atomic_chain(s,batches)
        require(type(chain) is Final,'OPG_REVISION_UNSEALED_RECONFIRM')
        return self._prepare(s,batches,actor.adapter,chain)

    def _prepare(self, s, batches, adapter, chain):
        self._pending=None
        now=self.wall(); m=self.source.manifest
        def authorized(t):
            return (get_ident()==self.owner and self.claims(s) and adapter is self.adapter
                    and self.bot._owned_revisit_no_positions is True
                    and self.bot._owned_revisit_reconciled is True
                    and m['reviewed_available_ms']<s.end<=t<min(m['use_until_ms'],s.expires)
                    and self.plan.current(s,t) is True and self.permitted(s,t) is True)
        require(authorized(now),'OPG_REVISION_ORIGINAL_AUTHORITY')
        p=self._prior(s.end,s.end)
        require(all(k.open_ms+k.width<=s.end-3_600_000 for k in self.source.keys),
                'OPG_REVISION_ENTRY_FACTS_OVERLAP')
        market=self.bot.exchange._api.markets.get(PAIR)
        require(type(market) is dict and market.get('id')==SYMBOL and market.get('symbol')==PAIR
                and market.get('quote')==market.get('settle')=='USDT'
                and all(market.get(k) is True for k in ('active','linear','swap'))
                and type(market.get('info')) is dict and market['info'].get('onboardDate')==p.origin,
                'OPG_REVISION_NATIVE_MARKET_IDENTITY')
        seg=self.store.segment(SYMBOL)
        require(seg is not None and seg.open_ms==p.origin and seg.proven_at_ms<=now,
                'OPG_REVISION_NATIVE_ORIGIN')
        rows=self.store.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(SYMBOL,)).fetchall()
        require(len(rows)==1,'OPG_REVISION_CANONICAL_RPD_ABSENT')
        r=RpdState.restore(rows[0][0])
        atr=AtrErCheckpoint(self.store)
        facts=tuple(atr._state(SYMBOL,w,seg) for w in (FIVE,QUARTER))
        require(r.observed<=now and all(a[1]<=now for a in facts),'OPG_REVISION_CANONICAL_CLOCK')
        cursors=(r.next_open,*[a[0] for a in facts])
        require((r.symbol,r.origin)==(SYMBOL,p.origin)
                and all(type(c) is int and p.origin<c<=p.cursor for c in cursors),
                'OPG_REVISION_ADVANCED_OR_UNKNOWN_CURSOR')
        require(s.end-p.cursor==QUARTER,'OPG_REVISION_ADJACENT_CARRY_REQUIRED')
        by=dict(batches)
        require(type(batches) is tuple and len(batches)==3 and set(by)=={MINUTE,FIVE,QUARTER},
                'OPG_REVISION_BATCH_SHAPE')
        tails={}
        for width in (FIVE,QUARTER):
            tail=tuple(q for q in by[width] if q.open_ms>=p.cursor)
            require(tuple(q.open_ms for q in tail)==tuple(range(p.cursor,s.end,width))
                    and all(s.end<=q.received_ms<=now for q in tail),'OPG_REVISION_NATIVE_TAIL')
            tails[width]=tail
        if chain is None:
            native=adapter.verified_committed_receipts(s)
        else:
            # Same arranged original chain: one immutable initial 1m/15m
            # plus exact confirmed 5m suffix, as publisher expects.
            native=tuple(x for x in chain.initial if x[0].ticket.lane in ('1m','15m'))+tuple(
                   x for x in chain.second if x[0].ticket.lane=='5m')
        require(type(native) is tuple and len(native)==3 and
                {r.ticket.lane for r,_ in native}=={'1m','5m','15m'},'OPG_REVISION_NATIVE_RECEIPT_REQUIRED')
        wires={}
        for receipt, sent in native:
            lane=receipt.ticket.lane
            w={'1m':MINUTE,'5m':FIVE,'15m':QUARTER}[lane]
            group=by[w]
            if chain is not None and w==FIVE:
                old,old_sent=next((r,t) for r,t in chain.initial if r.ticket.lane=='5m')
                require(len(receipt.raw)==3 and len(old.raw)==len(group) and old.ticket.scope==s
                        and HEX.fullmatch(old.source_proof) and s.end<=old_sent<=old.received_ms<=now,
                        'OPG_REVISION_RECONFIRM_PREFIX_ATTESTATION')
                require(self._row_match(group[:-3],old.raw[:-3],old.received_ms),
                        'OPG_REVISION_RECONFIRM_PREFIX_MISMATCH')
                group=group[-3:]
            require(receipt.ticket.scope==s and type(receipt.source_proof) is str
                    and HEX.fullmatch(receipt.source_proof)
                    and s.end<=sent<=receipt.received_ms<=now
                    and self._row_match(group,receipt.raw,receipt.received_ms),
                    'OPG_REVISION_NATIVE_RECEIPT_MISMATCH')
            wires[w]=(receipt.ticket.request_id,receipt.source_proof,sent,receipt.received_ms)
        receipts=digest(enc((PAIR,s.generation,s.end,s.expires,tuple(sorted(wires.items())),
               None if chain is None else chain.proof_sha256)))
        memory, ar, origin=restore_memory(p)
        try:
            for w, tail in tails.items():
                memory.record(SYMBOL,w,tail)
                require(ar.extend(SYMBOL,w,origin,s.end,now)==s.end,'OPG_REVISION_ATR_CURSOR')
            state=memory.rpd_progress(SYMBOL,s.end,now)
            require(state.next_open==s.end and state.closed().known is True,'OPG_REVISION_RPD_CURSOR')
            material=enc(dict(rpd_state=json.loads(state.pack()),atr={str(w):dict(
                  latest=ar.sample(SYMBOL,w,origin,s.end,now),
                  previous=ar.sample(SYMBOL,w,origin,s.end-w,now)) for w in (FIVE,QUARTER)}))
            data=json.loads(material)
            carrier=enc(dict(symbol=SYMBOL,origin_ms=p.origin,cursor_ms=s.end,recomputation_at_ms=now,
                derived_state=dict(rpd_state=data['rpd_state'],atr={str(w):dict(
                    state=ar._state(SYMBOL,w,origin)[2],latest=data['atr'][str(w)]['latest'],
                    prior=data['atr'][str(w)]['previous']) for w in (FIVE,QUARTER)})))
        finally:
            memory.db.close()
        require(authorized(self.wall()),'OPG_REVISION_ORIGINAL_AUTHORITY_LOST')
        v=FrozenReadView(PAIR,SYMBOL,s.generation,s.end,s.expires,self.owner,p.origin,now,
               m['reviewed_available_ms'],p.pin,receipts,digest(material),material,self.source.keys,batches)
        self._pending=(v,p,carrier,now)
        return v
