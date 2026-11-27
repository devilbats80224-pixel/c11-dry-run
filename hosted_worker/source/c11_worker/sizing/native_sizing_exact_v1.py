"""Entry-only reuse of pinned pure Decimal native sizing. No FT runtime/DB/RPC.
Called only for actual candidate entry attempts, never for each symbol/bar.
"""
from pathlib import Path
from decimal import Decimal,ROUND_DOWN
from exact_quantity_v1 import exact_plan_quantity
import importlib.util,sys,hashlib,json
ROOT=Path('/Volumes/DevilProjects/Workspace/2026-11-07/task-2/receipt_missing_fair_integration_v1/goal_five_capacity_acceptance_20261110_v1')
CAP=ROOT/'freqtrade_202610_ccxt90_image_v2/context/sources/2026-11-05/task-4/event_state_diagnostics_20261110_v1/image_integration/base_sources/app/paper_capacity_c3oi_capped.py'
INT=ROOT/'freqtrade_202610_release_payload_v1/payload/paper_p3_intent_c3oi_capped.py'
EXPECTED={str(CAP):'25b032f4dc647b17f2fb76f9b2b2de4ee7fbf47d3815e01224e658b99368e534',str(INT):'7495003c2f79cb4f3e61259e104c0c8c839e6dc6552bdb2a24541755b229bf7c'}
def load(name,path):
 assert hashlib.sha256(path.read_bytes()).hexdigest()==EXPECTED[str(path)], 'NATIVE_PURE_SOURCE_DRIFT'
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);sys.modules[name]=m;spec.loader.exec_module(m);return m
capacity=load('paper_capacity_c3oi_capped',CAP)
intents=load('_vbt_original_p3_intent',INT)
SOURCE_PINS={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [CAP,INT]}

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

def resolve_entry(pair,wallet,free,price,q5,q1h,lot,tick,minnotional,maxqty,minqty,maxcost):
 from native_risk import TIERS,max_leverage,buffered_liquidation
 import math
 proposed=min(wallet/3,free)
 L=min(5.,max(1.,max_leverage(pair,proposed)))
 allowed=[t['maxNotional'] for t in TIERS[pair] if t['maxLeverage']>=L and t['maxNotional'] is not None]
 if not allowed:raise ValueError('BRACKET_UNKNOWN')
 bracket=max(allowed)
 minstake=max(minnotional*min(max(1.05/(1-.06),1.),1.5),minqty*price*1.05)/L
 maxstake=min(bracket,maxqty*price,maxcost)/L
 raw,amount=size_entry(wallet,free,price,L,q5,q1h,maxqty,lot,tick,minnotional,bracket,minstake,maxstake)
 if amount<=0:return 0.,L,0.
 stake=float(Decimal(str(amount))*Decimal(str(price))/Decimal(str(L)))
 liq=buffered_liquidation(pair,price,amount,stake,tick)
 return amount,L,liq
