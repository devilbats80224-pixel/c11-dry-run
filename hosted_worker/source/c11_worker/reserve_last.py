"""Flat-only supplement must use a proven tagged vacancy when only one ordinary slot remains."""
def choose(slot,E,s,count):
 if count or len(slot.tags)!=2:return slot.allow(E,s,count)
 assert not slot.active and s not in slot.active and set(slot.tags)<{0,1,2}
 i=min(slot.tags,key=lambda k:(slot.tags[k][2],k));origin=slot.tags[i];assert origin[2]<=E
 assert (s,E) in slot.quote
 q=slot.quote[s,E]
 if q is None:label='SUPPLEMENT_LAST_ORDINARY_RESERVED_UNKNOWN'
 elif q['current']<=q['previous']:label='SUPPLEMENT_LAST_ORDINARY_RESERVED_NO_EXPANSION'
 else:label='TAGGED_QUOTE_EXPANSION'
 r=dict(E=E,symbol=s,slot=i,group=label,origin=origin,quote=q,actual_quantity_accepted=False)
 slot.audit.append(r)
 ok=label=='TAGGED_QUOTE_EXPANSION'
 slot.pending=(s,E,i,r) if ok else None
 return ok
