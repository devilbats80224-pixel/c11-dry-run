"""Operational 3-slot state; not an alternate portfolio."""
class SingleWalletSlots:
    def __init__(self,quote):
        self.active={}
        self.tags={}
        self.audit=[]
        self.quote=quote
        self.pending=None

    def exit(self,E,s,reason):
        slot,entry_E=self.active.pop(s)
        assert slot not in self.tags
        assert entry_E<E
        if reason=='WEAK_PROOF_ONLY_NEXT_OPEN':
            self.tags[slot]=(s,entry_E,E)

    def allow(self,E,s,real_open_count):
        assert s not in self.active and len(self.active)==real_open_count and real_open_count<3
        active={r[0] for r in self.active.values()}
        assert len(active)==real_open_count and not (active&set(self.tags))
        ordinary=sorted(set(range(3))-active-set(self.tags))
        if ordinary:
            slot=ordinary[0];out='ORDINARY_EXEMPT';q=None;asc=None
        else:
            slot=min(self.tags,key=lambda a:(self.tags[a][2],a));asc=self.tags[slot];assert asc[2]<=E
            assert (s,E) in self.quote
            q=self.quote[s,E]
            out='UNKNOWN_NO_REPLENISH' if q is None else ('TAGGED_QUOTE_EXPANSION' if q['current']>q['previous'] else 'TAGGED_NO_EXPANSION')
        self.audit.append(dict(E=E,symbol=s,slot=slot,group=out,origin=asc,quote=q,actual_quantity_accepted=False))
        ok=out in ('ORDINARY_EXEMPT','TAGGED_QUOTE_EXPANSION')
        self.pending=(s,E,slot,self.audit[-1]) if ok else None
        return ok

    def enter(self,E,s):
        ss,ee,slot,rec=self.pending
        assert (s,E)==(ss,ee)
        assert s not in self.active and slot not in {a[0]for a in self.active.values()}
        self.active[s]=(slot,E);self.tags.pop(slot,None);rec['actual_quantity_accepted']=True
        self.pending=None

def fixture():
    w=SingleWalletSlots({('E',3):{'previous':200.,'current':200.},('G',3):None,('H',3):{'previous':200.,'current':201.}})
    for s in ('A','B','C'):
        assert w.allow(1,s,len(w.active));w.enter(1,s)
    w.exit(2,'B','WEAK_PROOF_ONLY_NEXT_OPEN');w.exit(2,'C','OLD_STOP_INTRABAR')
    assert w.allow(3,'D',1) and w.audit[-1]['slot']==2 and w.audit[-1]['group']=='ORDINARY_EXEMPT'
    w.enter(3,'D');assert w.tags=={1:('B',1,2)}
    assert not w.allow(3,'E',2) and w.audit[-1]['group']=='TAGGED_NO_EXPANSION'
    assert not w.allow(3,'G',2) and w.audit[-1]['group']=='UNKNOWN_NO_REPLENISH'
    assert w.allow(3,'H',2) and w.audit[-1]['group']=='TAGGED_QUOTE_EXPANSION'
    assert w.tags=={1:('B',1,2)}  # sizing rejection does not clear tag
    w.enter(3,'H');assert not w.tags and w.active['H']==(1,3)
    print('8 operational assertions covered: normal first, weak-only tagging, observed clock, ties, unknown, no clear on sizing reject, tag cleared only when accepted, correct slot')
if __name__=='__main__':fixture()
