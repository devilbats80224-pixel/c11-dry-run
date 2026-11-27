"""Immutable actual C11 native dry-only entry controlled by an exact baked review."""
from hashlib import sha256
from importlib import import_module
from importlib.metadata import version
import json
import os
from pathlib import Path
import stat
import sys

HERE=Path('/opt/c11_owned_modules')
WORKER=Path('/opt/c11_worker')
def verify_sources():
    assert not sys.flags.optimize
    assert (os.getuid(),os.getgid())==(1000,2000)
    assert not any(k.startswith('FREQTRADE__') for k in os.environ)
    assert not any(Path(p).exists() for p in ('/run/openmarket-credential','/oi20-provider-secret','/var/run/docker.sock'))
    pin=HERE/'runtime_pins.json'
    mode=pin.lstat()
    assert stat.S_ISREG(mode.st_mode) and mode.st_uid==0 and mode.st_gid==0 and stat.S_IMODE(mode.st_mode)==0o444
    pins=json.loads(pin.read_bytes())
    assert len(pins)==43
    assert not (HERE/'__pycache__').exists()
    for name,expected in pins.items():
        src=Path(name)
        assert src.is_relative_to(HERE) or src.is_relative_to(WORKER)
        st=src.lstat()
        assert stat.S_ISREG(st.st_mode) and (st.st_uid,st.st_gid)==(0,0) and stat.S_IMODE(st.st_mode)==0o444
        assert sha256(src.read_bytes()).hexdigest()==expected
    return pins

def main():
    self_check=sys.argv[1:]==['--packaged-self-check']
    if sys.argv[1:] and not self_check:
        print('C11_UNRECOGNIZED_NATIVE_ENTRY',file=sys.stderr)
        return 2
    pins=verify_sources()
    import native_worker
    import c11_paper_exchange
    from inspect import signature
    from native_candles import NativeCandles
    assert 'fallback_lease_ns' in signature(NativeCandles.query).parameters
    assert callable(NativeCandles.update_nw)
    from entry_feed import EntryFeed
    from feature_warmup import FeatureWarmup
    from native_entry_owner import NativeEntryAdviceMixin
    assert 'verified_cycle_rows' in signature(FeatureWarmup.after_protection).parameters
    assert callable(EntryFeed.warmup_observation_since) and callable(NativeEntryAdviceMixin.analyze_pair)
    from freqtrade.resolvers import StrategyResolver
    from freqtrade.configuration.config_validation import validate_config_consistency
    from installed_build_inputs import installed_site_factory
    assert version('freqtrade')=='2026.10' and version('ccxt')=='4.5.90'
    assert native_worker.C11PaperExchange is c11_paper_exchange.C11PaperExchange
    assert callable(installed_site_factory)
    import sealed_owned_scheduler as s
    assert s.OWNED_SCHEDULER_FACTORY is s.OWNED_SCHEDULER_EXPECTED_SEAL is None
    for name in pins:
        p=Path(name)
        if p.parent in (HERE,WORKER) and p.name not in ('packaged_entry.py','runtime_pins.json'):
            assert Path(import_module(p.stem).__file__).resolve()==p
    cfg=native_worker.config_for(1000.0,['FOO/USDT:USDT'])
    strategy=StrategyResolver.load_strategy(cfg)
    assert type(strategy).__name__=='C11ManagedPaper'
    assert Path(str(strategy.__file__)).resolve()==WORKER/'strategy'/'Strategy.py'
    from native_entry_owner import NativeEntryAdviceMixin
    assert isinstance(strategy,NativeEntryAdviceMixin)
    validate_config_consistency(cfg)
    assert cfg['dry_run'] is True and cfg['runmode'].value=='dry_run'
    assert cfg['db_url']=='sqlite:////c11-paper/trades.c11.paper.sqlite'
    import baked_review_pin as build_pin
    assert Path(build_pin.__file__).resolve()==HERE/'baked_review_pin.py'
    m=(HERE/'baked_review_pin.py').lstat()
    assert stat.S_ISREG(m.st_mode) and (m.st_uid,m.st_gid)==(0,0) and stat.S_IMODE(m.st_mode)==0o444
    REVIEW_SHA,VERIFIED_RUNTIME_PINS_SHA=build_pin.REVIEW_SHA,build_pin.VERIFIED_RUNTIME_PINS_SHA
    assert not os.environ.get('PYTHONOPTIMIZE')
    assert sha256((HERE/'runtime_pins.json').read_bytes()).hexdigest()==VERIFIED_RUNTIME_PINS_SHA
    box=HERE/'review.json'
    src=box.read_bytes()
    assert len(src)<65536 and sha256(src).hexdigest()==REVIEW_SHA
    st=box.lstat()
    assert stat.S_ISREG(st.st_mode) and (st.st_uid,st.st_gid)==(0,0) and stat.S_IMODE(st.st_mode)==0o444
    d=json.loads(src)
    assert d['schema']=='c11-paper-native-owned-review-v1' and d['candidate_commit']=='7f39da38a993d270d465ea10d569b7b4b0380785'
    assert d['runtime_manifest_sha']==VERIFIED_RUNTIME_PINS_SHA
    assert d['run_mode']=='C11_PAPER_ONLY' and d['public_path']=='public-binance-fapi-get-only'
    from re import fullmatch
    assert d['stage']=='SCOPED_C11_PAPER_RELEASE' and fullmatch(
        r'Sentinel_[a-f0-9]{32}',d['activation_authorization'])
    assert d['preceding_reviewed_candidate_image']=='sha256:92cfea3e30d0e1af3bd344ce3991ecdc7398d3e59fe8f2bc7ba7c98f882aae19'
    assert d['paper_ledger']=='/c11-paper/trades.c11.paper.sqlite'
    assert d['native_observer']=='observer-native-owned-v1.sqlite'
    from startup_status import ARCHIVE, LEDGER
    assert ARCHIVE==d['native_observer'] and LEDGER=='trades.c11.paper.sqlite'
    from c11_v4_pin import STAGE_ONLY, REVIEWED_MOUNT, REVIEWED_SCOPE, RELEASE_SHA256
    from release_facts import require_release_facts
    from docker_observer import binding_for
    from v4_entry import verify_runtime, run_locked
    assert STAGE_ONLY is False and len(REVIEWED_MOUNT)==4
    assert REVIEWED_SCOPE['publication_binding']==binding_for(REVIEWED_MOUNT)==d['inherited_original_publication_binding']
    assert RELEASE_SHA256==d['inherited_original_receipt_sha']
    # The inherited release attests only the unchanged same-ledger & data
    # placement. New source and proof pins above are independently reviewed.
    require_release_facts('/opt/c11-release',RELEASE_SHA256,REVIEWED_SCOPE)
    if self_check:
        print('C11_EXACT_PACKAGED_REVIEWED_NATIVE_OVERLAY_PASS')
        return 0
    verify_runtime()
    os.umask(0o077)
    from paper_runtime import PaperState, verify_state_file
    from docker_c11_intake import open_intake
    from paper_standalone_entry import startup_config
    from standalone_policy import build_policy
    def record(_pair,status):
        if type(status) is str and (status=='ORIGINAL_NATIVE_CALLBACK' or status.startswith('HOLD_')) and status.isascii() and status.replace('_','').isalnum() and len(status)<=96:
            print(json.dumps({'component':'C11_ORIGINAL_NATIVE_PRIMARY','status':status}),flush=True)
    assert native_worker.OWNED_SCHEDULER_FACTORY is native_worker.OWNED_SCHEDULER_EXPECTED_SEAL is None
    native_worker.OWNED_SCHEDULER_FACTORY=installed_site_factory(REVIEW_SHA,record)
    native_worker.OWNED_SCHEDULER_EXPECTED_SEAL=REVIEW_SHA
    run_locked(REVIEWED_SCOPE,REVIEWED_MOUNT,PaperState,verify_state_file,open_intake,startup_config,
               build_policy,native_worker.PaperWorker)
    return 0
if __name__=='__main__':raise SystemExit(main())
