"""Same packaged approved OPG prior plus real binding negative controls."""
from hashlib import sha256
import json
from pathlib import Path

from binding import Evidence, bind
from rpd_checkpoint import RpdState
from stage_pin import CATALOG_SHA, AUTH_SHA

root=Path("/opt/c11-reviewed-seeds")
symbol,pair="OPGUSDT","OPG/USDT:USDT"
review=(root/"review.json").read_bytes()
assert AUTH_SHA is None and sha256(review).hexdigest()==CATALOG_SHA
slots=json.loads(review)["slots"]
assert set(slots)=={"BABYUSDT","MMTUSDT",symbol,"PHAROSUSDT","RPLUSDT","SAHARAUSDT","STBLUSDT"}
slot=slots[symbol]
assert (slot["approved_at_ms"],slot["cursor_ms"])==(1795580487334,1795575600000)
assets={"seed":(root/(symbol+".seed.json")).read_bytes(),
        "independent_reproduction":(root/(symbol+".reproduction.json")).read_bytes()}
source=json.loads(assets["seed"])
assert source["artifact_available_ms"]==1795579169034<slot["approved_at_ms"]

def verify(end,*,current=True,mutated=False):
    time=end+1000
    def read(role,label):
        if label!=symbol:return None
        raw=assets[role]
        return raw+b" " if mutated and role=="seed" else raw
    return bind(Evidence(1,symbol,pair,end,time,end+60_000),now_ms=time,
        identity=source["contract"]["payload"],review=review,review_sha=CATALOG_SHA,
        read_asset=read,is_current=lambda *_:current,
        native_validate=lambda raw:RpdState.restore(raw.decode()).pack().encode(),
        wall_now=lambda:time)

for old in (1795575600000,1795580100000):
    assert verify(old)==(None,"SOURCE_RECORD_PIN")
accepted=1795581000000
state,status=verify(accepted)
assert status=="PRIOR_CURSOR_SOURCE_BOUND_ONLY" and state.cursor_ms==slot["cursor_ms"] and state.tail_bars==18
assert verify(accepted,current=False)==(None,"OWNER_AUTHORITY_INVALID")
assert verify(accepted,mutated=True)==(None,"ASSET_PIN")
print("C11_REVIEWED_OPG_PRIOR_CATALOG_UNCHANGED_NOT_OPERATIONAL")
