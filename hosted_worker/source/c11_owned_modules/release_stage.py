"""Immutable release-review candidate. No operational entry or inherited approval."""
from hashlib import sha256
from importlib.metadata import version
from inspect import signature
from pathlib import Path
import json,os,stat,sys
from stage_pin import RUNTIME_SHA,CATALOG_SHA,AUTH_SHA

def check(*, verify_cold_content=True):
    assert sys.flags.optimize==0 and (os.getuid(),os.getgid())==(1000,2000)
    assert not any(k.startswith('FREQTRADE__') for k in os.environ)
    assert not any(Path(p).exists() for p in ('/run/openmarket-credential','/oi20-provider-secret','/var/run/docker.sock'))
    def checked(p):
       m=p.lstat()
       assert stat.S_ISREG(m.st_mode) and (m.st_uid,m.st_gid)==(0,0) and stat.S_IMODE(m.st_mode)==0o444
       assert os.statvfs(p).f_flag&os.ST_RDONLY
       return p.read_bytes()
    d=checked(Path('/opt/c11_owned_modules/release_stage_runtime.json'))
    assert sha256(d).hexdigest()==RUNTIME_SHA
    m=json.loads(d)
    assert m['release_enabled'] is False
    for n,h in m['files'].items():assert sha256(checked(Path(n))).hexdigest()==h
    from cold_archive_release import installed_sources
    import time
    sources=installed_sources(m,time.time_ns()//1_000_000,verify_content=verify_cold_content)
    from cold_archive_binding import MODE_KEY, plan as cold_archive_plan
    if sources or MODE_KEY in m:
        from stage_pin import HISTORY_PREP_ENABLED
        assert HISTORY_PREP_ENABLED is True
    print('C11_COLD_ARCHIVE_REVIEWED_SOURCES',len(sources),flush=True)
    cat=Path('/opt/c11-reviewed-seeds')
    assert not (cat/'QUARANTINE.json').exists()
    assert sha256(checked(cat/'review.json')).hexdigest()==CATALOG_SHA
    from freqtrade.configuration.config_validation import validate_config_consistency
    from freqtrade.resolvers import StrategyResolver
    from native_worker import config_for,OWNED_SCHEDULER_FACTORY,OWNED_SCHEDULER_EXPECTED_SEAL
    from c11_paper_exchange import C11PaperExchange
    from installed_build_inputs import installed_site_factory
    from v4_entry import run_locked
    from outer_seed_lock import ScopedSeedFactory
    assert OWNED_SCHEDULER_FACTORY is OWNED_SCHEDULER_EXPECTED_SEAL is None
    assert 'prior_seed_factory' in signature(installed_site_factory).parameters and {'seed_scope','one_shot'}<=signature(run_locked).parameters.keys()
    assert version('freqtrade')=='2026.10' and version('ccxt')=='4.5.90'
    cfg=config_for(1000.0,['FOO/USDT:USDT'])
    st=StrategyResolver.load_strategy(cfg)
    validate_config_consistency(cfg)
    assert type(st).__name__=='C11ManagedPaper'
    assert cfg['dry_run'] is True and cfg['runmode'].value=='dry_run' and cfg['db_url']=='sqlite:////c11-paper/trades.c11.paper.sqlite'
    from authorization_boundary import validate_baked
    assert not validate_baked(None,AUTH_SHA,RUNTIME_SHA,CATALOG_SHA,0)
    if verify_cold_content and MODE_KEY in m:
        from history_factory_admission import FactoryAdmission
        from cold_archive_binding import ColdSourceAdmission
        now=lambda:time.time_ns()//1_000_000
        prep=cold_archive_plan(m,sources,now,HISTORY_PREP_ENABLED)
        admission=prep.cold_admission(now)
        try:
            assert type(admission) is ColdSourceAdmission
            assert callable(installed_site_factory(RUNTIME_SHA,lambda *_:None,history_startup=FactoryAdmission(RUNTIME_SHA),
                                      prepared_history_quarantine=(),archive_prep=prep))
        finally:
            admission.close()
            prep.close()
        print('C11_PUBLIC_COLD_ARCHIVE_OFFICIAL_SITE_FACTORY_READY_DISABLED',flush=True)
    print('REVIEW_STAGE_IMAGE_NATIVE_SCHEMA_AND_PINS_PASS',RUNTIME_SHA,CATALOG_SHA,flush=True)

    return m

def verify_activation_assets():
    # This same check is exercised in the final packaged-image regression.
    # It is before any ledger access and cannot grant runtime authority.
    from c11_v4_pin import STAGE_ONLY, REVIEWED_MOUNT, REVIEWED_SCOPE, RELEASE_SHA256
    from release_facts import require_release_facts
    from docker_observer import binding_for
    from v4_entry import verify_runtime

    assert STAGE_ONLY is False and len(REVIEWED_MOUNT) == 4
    assert REVIEWED_SCOPE['publication_binding'] == binding_for(REVIEWED_MOUNT)
    routes = require_release_facts('/opt/c11-release', RELEASE_SHA256, REVIEWED_SCOPE)
    verify_runtime()
    return routes


def activate():
    # No env or CLI secrets. Fresh owner-reviewed thin layer only adds the
    # separate immutable approval file and its digest baked into stage_pin.
    if AUTH_SHA is None:
       print('C11_NO_NEW_BAKED_OWNER_AUTHORIZATION_NO_OPERATIONAL_ENTRY',file=sys.stderr,flush=True)
       return 2
    verified_manifest=check(verify_cold_content=False)
    import time
    from authorization_boundary import denial_reason
    from one_shot import OneShot
    from retained_diagnostic import redacted_fd2_only
    manifest=Path('/opt/c11_owned_modules/fresh_owner_scope.json')
    m=manifest.lstat()
    assert stat.S_ISREG(m.st_mode) and m.st_nlink==1 and (m.st_uid,m.st_gid)==(0,0) and stat.S_IMODE(m.st_mode)==0o444 and 0<m.st_size<=16384 and os.statvfs(manifest).f_flag&os.ST_RDONLY
    data=manifest.read_bytes()
    now=lambda:time.time_ns()//1_000_000
    reason=denial_reason(data,AUTH_SHA,RUNTIME_SHA,CATALOG_SHA,now())
    if reason is not None:
       print('C11_BAKED_OWNER_SCOPE_REFUSED',reason,file=sys.stderr,flush=True)
       return 2
    from cold_archive_release import installed_sources
    sources=installed_sources(verified_manifest,now())
    from c11_v4_pin import REVIEWED_MOUNT, REVIEWED_SCOPE
    from v4_entry import run_locked
    from paper_runtime import PaperState,verify_state_file
    from docker_c11_intake import open_intake
    from paper_standalone_entry import startup_config
    from standalone_policy import build_policy
    from installed_build_inputs import installed_site_factory
    from history_factory_admission import FactoryAdmission
    from outer_seed_lock import ScopedSeedFactory
    import native_worker
    verify_activation_assets()
    from outer_seed_lock import private_identity,LEDGER,GUARD_PATH,LOCK
    for p in (LEDGER,GUARD_PATH,LOCK):private_identity(p)
    seed=c11_locked_scope()
    from stage_pin import OPG_REVISION_REVIEW_SHA
    from reviewed_opg_selector import installed_review
    revision=installed_review(OPG_REVISION_REVIEW_SHA,RUNTIME_SHA,now())
    proof=OneShot(seed,data,AUTH_SHA,RUNTIME_SHA,CATALOG_SHA,now)
    # Reviewed launcher pins exact OCI ID. Verified installed byte/runtime
    # checks above own this single typed process-local channel.
    history_channel=FactoryAdmission(RUNTIME_SHA)
    sink,close_sink=redacted_fd2_only()
    def status(label):
       sink((json.dumps({'schema':'c11-reviewed-startup-info-v1','state':label},separators=(',',':'))+'\n').encode())
    last_phase='REVIEWED_LOCKED_START'
    allowed_phases=frozenset(('PAPER_WRITER_CHECK','SEED_SCOPE_CHECK','STATE_FILES_CHECK','READONLY_LINEAGE_PRECHECK',
        'ONE_SHOT_CONSUME','SCOPED_RELEASE_START','POST_CONSUME_LINEAGE_CHECK','OPEN_AUTHENTIC_SOURCE',
        'NATIVE_CONFIG','NATIVE_WORKER_CONSTRUCTOR','NATIVE_WORKER_RUN'))
    def phase(label):
       nonlocal last_phase
       if label=='READONLY_LINEAGE_REVERIFIED':status(label)
       if label in allowed_phases:last_phase=label
    try:
       def record(_pair,_status):return None
       assert native_worker.OWNED_SCHEDULER_FACTORY is native_worker.OWNED_SCHEDULER_EXPECTED_SEAL is None
       from stage_pin import HISTORY_PREP_ENABLED
       # Use only the original baked/validated scope. Never infer approved exclusions.
       reviewed_history=tuple(json.loads(data)['quarantined']) if HISTORY_PREP_ENABLED is True else None
       from cold_archive_binding import plan as cold_archive_plan
       archive_prep=cold_archive_plan(verified_manifest,sources,now,HISTORY_PREP_ENABLED)
       native_worker.OWNED_SCHEDULER_FACTORY=installed_site_factory(RUNTIME_SHA,record,diagnostic_sink=sink,
           prior_seed_factory=seed.open,history_startup=history_channel,prepared_history_quarantine=reviewed_history,reviewed_revision=revision,archive_prep=archive_prep)
       native_worker.OWNED_SCHEDULER_EXPECTED_SEAL=RUNTIME_SHA
       status('VERIFIED_FRESH_REVIEW_ENTERING_EXCLUSIVE_STATE')
       run_locked(REVIEWED_SCOPE,REVIEWED_MOUNT,PaperState,verify_state_file,open_intake,startup_config,
                  build_policy,native_worker.PaperWorker,seed_scope=seed,one_shot=proof,diagnostic=phase,history_startup=history_channel)
       status('NATIVE_PAPER_RUN_RETURNED')
       return 0
    except Exception as exc:
       from startup_status import code
       # Only fixed, allowlisted labels. Never emit exception text, file
       # names, scope contents, hashes, or the one-use marker name.
       status('PAPER_STARTUP_REFUSED_'+last_phase+'_'+code(exc))
       status('PAPER_STARTUP_REFUSED_OR_STOPPED')
       return 2
    finally:close_sink()

def c11_locked_scope():
    from outer_seed_lock import ScopedSeedFactory
    return ScopedSeedFactory(review_catalog=Path('/opt/c11-reviewed-seeds'),review_pin=CATALOG_SHA,
                runtime_manifest_sha=RUNTIME_SHA,expected_manifest_sha=RUNTIME_SHA,isolated_test=False)

if __name__=='__main__':
    if sys.argv[1:]==['--review-self-check']:check()
    elif not sys.argv[1:]:raise SystemExit(activate())
    else:
       print('C11_NO_NEW_BAKED_OWNER_AUTHORIZATION_NO_OPERATIONAL_ENTRY',file=sys.stderr,flush=True)
       raise SystemExit(2)
