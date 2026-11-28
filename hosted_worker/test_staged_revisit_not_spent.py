"""Exercise the actually installed NativeEntryOwner with disposable test data."""
from types import SimpleNamespace
from pandas import DataFrame, to_datetime

from native_entry_owner import NativeEntryOwner

E = 1795823100000
Q = 900_000
PAIR = "SYNTH/USDT:USDT"
clock = E + 7200
frame = DataFrame({"date": to_datetime([E-Q], unit="ms", utc=True),
                   "open": [1.], "high": [1.], "low": [1.], "close": [1.], "volume": [1.]})
proof = SimpleNamespace(pair=PAIR, end=E, expires=E+55_000, generation=1)
phase = {"complete": False}
notes = []


def ready(p, pair, end):
    return p is proof and pair == PAIR and end == E and clock < proof.expires


def prepare(p, f):
    assert p is proof and f is frame
    return ("exact-native-frame",) if phase["complete"] else None


owner = NativeEntryOwner(trusted_owner_input=lambda *_: None, record=lambda *_: None,
                         revisit_ready=ready, revisit_frame=prepare,
                         revisit_frame_current=lambda p: p is proof,
                         revisit_diagnostic=lambda p, why: notes.append((p, why)))
# The buggy implementation ignored this scoped requirement and spent the
# genuine Freqtrade revisit even though staging had produced no frame.
owner.requires_staged_revisit_frame = lambda p: p is proof
owner.holds[PAIR] = proof
strategy = SimpleNamespace(timeframe="15m", config={"dry_run": True, "candle_type_def": "futures"},
                           dp=SimpleNamespace(ohlcv=lambda *_, **__: frame))
assert not owner.one_public_revisit(strategy, PAIR), "C11_REVISIT_SPENT_BEFORE_FRAME_WAS_STAGED"
assert owner.holds.get(PAIR) is proof and (PAIR, E) not in owner.spent
assert notes == [(proof, "REVISIT_STAGED_FRAME_PENDING")]
phase["complete"] = True
assert owner.one_public_revisit(strategy, PAIR) and owner.staged_frame==(proof, ("exact-native-frame",))
assert not owner.one_public_revisit(strategy, PAIR), "C11_REVISIT_ALREADY_RESERVED_DUPLICATED"
# The same bytes after the original lease cannot win a revisit.
owner.spent.clear()
owner.holds[PAIR] = proof
clock = proof.expires
assert not owner.one_public_revisit(strategy, PAIR)
assert (PAIR, E) not in owner.spent
print("C11_STAGING_WAIT_PRESERVES_ORIGINAL_SAME_CLOSE_REVISIT")


def same_close_warm_receipt_failure_is_explained_before_source_expiry():
    from coordinator import Scope
    from warm_release_binding import WarmReleaseBinding

    s=Scope(PAIR,"SYNTHUSDT",7,E,E+11949)
    now=[E+7562]
    remaining=[29]
    diagnostic=[]
    ready=SimpleNamespace(latency_budget=30,effective_expiry=lambda scope,_:scope.expires,
                          authority=lambda scope,_:scope is s)
    bot=SimpleNamespace(config={"dry_run":True},_owned_revisit_no_positions=True,
                        _owned_revisit_reconciled=True)
    adapter=SimpleNamespace(ready=ready,_proven={})
    scheduler=SimpleNamespace(wall=lambda:now[0],marker=(s.generation,E),
        covers={PAIR:s},worker=SimpleNamespace(freqtrade=bot),adapter=adapter,
        commit_and_decide=SimpleNamespace(remaining=lambda:remaining[0]),
        _transition=lambda scope,why:diagnostic.append((scope,why)),
        queue=SimpleNamespace(accept_completed_original_scope=lambda *_:None),tickets={})
    branch=WarmReleaseBinding(scheduler)
    branch.selected.add(s)
    branch.prefix[s]=((),())
    assert not branch.revisit(s,PAIR,E)
    assert diagnostic==[(s,'WARM_REVISIT_OWNER_HEADROOM')], 'C11_VISIBLE_RECEIPT_STAGE_FAILURE_UNEXPLAINED'
    assert branch.prepared is None and now[0]<s.expires
    remaining[0]=100
    assert not branch.revisit(s,PAIR,E)
    assert diagnostic[-1]==(s,'WARM_REVISIT_NO_WITNESSED_MINUTE')
    print("C11_WARM_STAGE_REFUSAL_REPORTED_BEFORE_SOURCE_EXPIRY")


same_close_warm_receipt_failure_is_explained_before_source_expiry()


def native_crossing_rechecks_the_verified_ready_clock():
    from primary_coverage_gate import NativePrimaryEvidence,NativePrimaryHandoff
    wall=[E+68_200]
    row=SimpleNamespace(symbol="SYNTHUSDT",generation=8)
    material=((),(),"sealed")
    rpd=object()
    evidence=NativePrimaryEvidence(PAIR,row.symbol,row.generation,E,E+68_100,E+110_000,
                                   "same-native-frame",material,rpd)
    allowed=[True]
    gate=NativePrimaryHandoff(wall=lambda:wall[0],
        original_scope=lambda c,proof,t: c is row and proof is evidence and allowed[0],
        frame_sha=lambda p,e:"same-native-frame")
    with gate.committed(evidence):
        assert gate.resolve(PAIR,row,E) is None,"C11_RAW_REPLAY_CAN_FORGE_READY_CLOCK"
    gate.verified_ready_at=lambda c,proof,t: E+68_000 if c is row and proof is evidence else None
    with gate.committed(evidence):
        assert gate.resolve(PAIR,row,E)==(material,rpd),"C11_NATIVE_CROSSING_STILL_USES_WIRE_CLOCK"
        allowed[0]=False
        assert gate.resolve(PAIR,row,E) is None,"C11_LATE_NATIVE_CROSSING_USED_STALE_RANK"
        allowed[0]=True
        wall[0]=evidence.expires_ms
        assert gate.resolve(PAIR,row,E) is None,"C11_LATE_NATIVE_CROSSING_USED_EXPIRED_AUTHORIZATION"
    print("C11_NATIVE_CROSSING_USES_ONLY_VERIFIED_READY_CLOCK_AND_FRESH_RANK")


native_crossing_rechecks_the_verified_ready_clock()


def durable_projection_rechecks_verified_ready_clock():
    from committed_projection import AuthoritativePrimaryCommit,CommittedProjection
    row=SimpleNamespace(pair=PAIR,symbol="SYNTHUSDT",generation=8)
    k=(row.pair,row.symbol,row.generation,E)
    t=E+68_200
    receipt=AuthoritativePrimaryCommit(*k,E+68_100,E+110_000,E+68_000,
                                      *("a"*64,)*5)
    trusted=[True]
    projection=CommittedProjection(wall=lambda:t,native_owner_lookup=lambda *x:receipt if x==k else None,
        current_authority=lambda c,r,now:c is row and r is receipt and trusted[0],
        trusted_rpd_pin=lambda *_:"a"*64)
    assert not projection._valid(receipt,k,row,t)
    projection.verified_ready_at=lambda c,r,now:E+68_000 if c is row and r is receipt else None
    assert projection._valid(receipt,k,row,t), "C11_DURABLE_PROJECTION_STILL_USES_WIRE_CLOCK"
    trusted[0]=False
    assert not projection._valid(receipt,k,row,t)
    trusted[0]=True
    assert not projection._valid(receipt,k,row,receipt.expires_ms)
    print("C11_DURABLE_PROJECTION_USES_VERIFIED_READY_OWNER_WINDOW")


durable_projection_rechecks_verified_ready_clock()


def native_policy_reads_late_inputs_only_through_the_authenticated_handoff():
    from unittest.mock import patch
    from primary_coverage_gate import NativePrimaryEvidence,NativePrimaryHandoff
    from standalone_policy import StandalonePolicy
    now=E+68_200
    expires=E+110_000
    row=SimpleNamespace(symbol="SYNTHUSDT",generation=8)
    primary=((),(),"sealed")
    rpd=object()
    event=NativePrimaryEvidence(PAIR,row.symbol,row.generation,E,E+68_100,expires,
                                "same-native-frame",primary,rpd)
    active=[True]
    handoff=NativePrimaryHandoff(wall=lambda:now,
        original_scope=lambda c,p,t:c is row and p is event and active[0],
        frame_sha=lambda p,e:"same-native-frame",
        verified_ready_at=lambda c,p,t:E+68_000 if c is row and p is event else None)
    seen=[]
    def inputs(symbol,end,asof,*,primary,state,valid_until_ms=None):
        seen.append((symbol,end,asof,primary,state,valid_until_ms))
        return {"valid_until_ms":valid_until_ms}
    def no_wire(*_):
        raise AssertionError("C11_LATE_NATIVE_PRIMARY_OPENED_NEW_MARKET_WIRE")
    policy=object.__new__(StandalonePolicy)
    policy.slots=SimpleNamespace(ready=True)
    policy.bars=SimpleNamespace(inputs=inputs)
    policy.candles=SimpleNamespace(update_entry_hour=no_wire)
    bot=SimpleNamespace(_c11_native_primary_handoff=handoff,config={'dry_run':True},
                        native_paper_exchange=lambda:True)
    with patch("standalone_policy.time.time_ns",return_value=now*1_000_000):
        assert policy.primary_inputs(bot,PAIR,row) is None
        with handoff.committed(event):
            result=policy.primary_inputs(bot,PAIR,row)
            assert result=={"valid_until_ms":expires}, "C11_NATIVE_POLICY_STILL_DROPS_VERIFIED_LATE_INPUTS"
            assert seen==[(row.symbol,E,now,primary,rpd,expires)]
            active[0]=False
            assert policy.primary_inputs(bot,PAIR,row) is None
    print("C11_NATIVE_POLICY_CONSUMES_AUTHENTICATED_READY_SCOPE_WITHOUT_WIRE")


native_policy_reads_late_inputs_only_through_the_authenticated_handoff()
