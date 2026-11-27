"""Real installed startup authorization rejects a different predecessor."""
from hashlib import sha256
import json
from pathlib import Path
import runpy
import sys
from unittest.mock import patch

sys.path[:0] = ["/opt/c11_owned_modules", "/opt/c11_worker", "/opt/c11_worker/ports"]
import stage_pin
from authorization_boundary import denial_reason,validate_baked

ROOT = Path("/c11-ci-tools/pin_postmerge_parent.py")
helper = runpy.run_path(str(ROOT))["bind_scope"]
A = "sha256:" + "a" * 64
B = "sha256:" + "b" * 64
R, C = "c" * 64, "d" * 64
SCOPE = dict(schema="C11_COMPATIBLE_SEVEN_PAPER_OPG_PRIOR_ROLL_FORWARD_V1",
             verified_owner_event="Sentinel_" + "1" * 32, parent_image=A,
             candidate_release_manifest=R, seed_catalog_sha=C,
             paper_ledger="/c11-paper/trades.c11.paper.sqlite",
             causal_store="/c11-paper/causal-candles.sqlite",
             accepted_cohort=["BABYUSDT","CYSUSDT","MMTUSDT","OPGUSDT","PHAROSUSDT","RPLUSDT","SAHARAUSDT"],
             quarantined=["AIOTUSDT","LISTAUSDT"], nonce="9" * 64,
             reviewed_from_ms=1_245_000, reviewed_until_ms=1_300_000,
             no_real_orders=True, no_oi_change=True, redacted_log_mode="bounded_docker_local_v1")
def raw(**updates):
    return json.dumps({**SCOPE, **updates}, separators=(",", ":")).encode()
def sha(data):
    return sha256(data).hexdigest()
def check(data, at=1_250_000):
    return validate_baked(data, sha(data), R, C, at)
data = raw()
with patch.object(stage_pin,"RELEASE_PARENT_IMAGE",A,create=True):
    reason=denial_reason(data,sha(data),R,C,1_250_000)
    assert reason is None, "C11_REVIEWED_CURRENT_PREDECESSOR_REFUSED:"+str(reason)
    assert not check(raw(parent_image=B))
    typo = raw(accepted_cohort=[*SCOPE["accepted_cohort"][:5], "RPLUSUSDT", "SAHARAUSDT"])
    assert denial_reason(typo, sha(typo), R, C, 1_250_000) == "ACCEPTED_SCOPE_LIST_MISMATCH"
    assert not check(raw(no_real_orders=False))
    assert not check(data, 1_300_000)
    assert not validate_baked(data, "8"*64,R,C,1_250_000)
with patch.object(stage_pin,"RELEASE_PARENT_IMAGE",None,create=True):
    assert not check(data)
with patch.object(stage_pin,"RELEASE_PARENT_IMAGE",B,create=True):
    assert check(raw(parent_image=B))
    assert not check(data)
stage = f"AUTH_SHA={sha(data)!r}\nRUNTIME_SHA={R!r}\nCATALOG_SHA={C!r}\n"
assert f"RELEASE_PARENT_IMAGE={A!r}" in helper(stage,data,A)
for supplied_stage, supplied_parent in ((stage,B),(stage+f"RELEASE_PARENT_IMAGE={B!r}\n",A)):
    try:
        helper(supplied_stage,data,supplied_parent)
    except ValueError:
        pass
    else:
        raise AssertionError("C11_POSTMERGE_SCOPE_PINNED_WRONG_IMAGE")
print("C11_REVIEWED_CURRENT_PREDECESSOR_AND_WRONG_PARENT_CONTROL_PASS")
