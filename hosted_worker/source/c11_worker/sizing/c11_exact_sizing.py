from decimal import Decimal as D, Decimal, ROUND_DOWN
import math
from exact_quantity_v1 import exact_plan_quantity
import paper_p3_intent_c3oi_capped as intents
class InputUnknown(ValueError): pass

def size_entry(wallet,free,price,leverage,q5,q1h,maxqty,step,tick,minnotional,bracket,minstake,maxstake):
 # Inputs min/maxstake and leverage must be derived from original tier/market rules.
 proposed=min(wallet/3,free);requested=min(proposed,maxstake,free)
 if requested<=0:return 0.,0.
 obj=intents.PaperP3Intents()
 try:
  plan=obj.prepare(pair='BATCH/USDT:USDT',E_ms=0,side='long',realized_wallet=wallet,available_margin=free,pending_margin=0,
    requested_margin=requested,leverage=leverage,entry_price=price,quote_volume_5m=q5,quote_volume_1h=q1h,
    market_max_quantity=maxqty,quantity_step=step,price_tick=tick,min_notional=minnotional,eligible_notional_bracket=bracket,tier_quality='BUNDLED_STATIC_PROXY')
 except (ValueError,ArithmeticError):return 0.,0.
 resolved=float(plan['resolved_margin']);maxallowed=min(maxstake,free)
 # Native Wallets.validate_stake_amount(initial entry) branches, no position adjustment.
 if minstake>maxallowed:return 0.,0.
 if resolved<minstake:
  if resolved*1.3<minstake:return 0.,0.
  resolved=minstake
 resolved=min(resolved,maxallowed)
 raw=(resolved/price)*leverage
 try:planned_raw=exact_plan_quantity(plan,resolved,price,leverage,step)
 except (ValueError,ArithmeticError):return raw,0.
 if planned_raw is not None:raw=planned_raw
 check=obj.confirm(pair='BATCH/USDT:USDT',E_ms=0,side='long',quantity=raw,entry_price=price,realized_wallet=wallet,available_margin=free,pending_margin=0)
 if not check['permit']:return raw,0.
 # Current contracts are exactly1; Decimal(str) truncate is tick-size CCXT semantics.
 final=float((Decimal(str(raw))/Decimal(str(step))).to_integral_value(rounding=ROUND_DOWN)*Decimal(str(step)))
 return raw,final

def fixed5_plan(pair,wallet,free,price,q5,q1h,meta,tiers):
    # Reconstructed fixed-5x wrapper: unlike old resolve_entry, never fall back.
    if not tiers or any(not all(k in t for k in ('maxLeverage','minNotional','maxNotional')) for t in tiers):raise InputUnknown('TIERS_UNKNOWN')
    for t in tiers:
        if not all(isinstance(t[k],(int,float)) and math.isfinite(t[k]) for k in ('maxLeverage','minNotional','maxNotional')):raise InputUnknown('TIER_NONFINITE_OR_UNBOUNDED')
        if not (t['maxLeverage']>=1 and 0<=t['minNotional']<t['maxNotional']):raise InputUnknown('TIER_INVALID')
    ordered=sorted(tiers,key=lambda t:t['minNotional'])
    if ordered[0]['minNotional']!=0 or any(a['maxNotional']!=b['minNotional'] for a,b in zip(ordered,ordered[1:])):raise InputUnknown('TIER_COVERAGE_GAP')
    allow=[t['maxNotional'] for t in ordered if t['maxLeverage']>=5]
    if not allow:return {'quantity':0.,'reason':'COMPLETE_TIERS_MAX_LEVERAGE_BELOW5','leverage':5}
    required=('lot','tick','minnotional','maxqty','minqty','maxcost')
    if any(k not in meta for k in required):raise InputUnknown('MARKET_METADATA_MISSING')
    if math.isnan(meta['maxcost']) or meta['maxcost']<=0:raise InputUnknown('MAXCOST_INVALID')
    if not (meta['minnotional']>=0 and meta['maxqty']>0 and 0<=meta['minqty']<=meta['maxqty']):raise InputUnknown('MARKET_LIMITS_INVALID')
    vals=[wallet,free,price,q5,q1h,meta['lot'],meta['tick'],meta['minnotional'],meta['maxqty'],meta['minqty']]
    if not all(math.isfinite(x) for x in vals) or min(wallet,free,q5,q1h)<0 or min(price,meta['lot'],meta['tick'])<=0:raise InputUnknown('SIZING_INPUT_UNKNOWN')
    bracket=max(allow)
    # Exact older native validation reserve; .06 is its stake validation parameter,
    # NOT the position hard7 stop and NOT a risk-distance size scaler.
    minstake=max(meta['minnotional']*min(max(1.05/(1-.06),1.),1.5),meta['minqty']*price*1.05)/5
    maxstake=min(bracket,meta['maxqty']*price,meta.get('maxcost',float('inf')))/5
    raw,q=size_entry(wallet,free,price,5,q5,q1h,meta['maxqty'],meta['lot'],meta['tick'],meta['minnotional'],bracket,minstake,maxstake)
    if q>0:
        notional=D(str(q))*D(str(price))
        if not any(D(str(t['minNotional']))<=notional<=D(str(t['maxNotional'])) and t['maxLeverage']>=5 for t in ordered):raise InputUnknown('FINAL_NOTIONAL_TIER_NOT_SUPPORTED')
        assert D(str(q))%D(str(meta['lot']))==0
    return {'quantity':q,'raw_confirmed_quantity':raw,'leverage':5,'notional':q*price,'margin':q*price/5,'target_margin':min(wallet/3,free),'bracket5':bracket,'reason':'ACCEPTED' if q>0 else 'NATIVE_CAPACITY_REJECT','stop_distance_size_scaling':False}
