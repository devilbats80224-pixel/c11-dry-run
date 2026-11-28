"""C11 V4 staged entry, default deliberately refuses any deployment."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import sys
import time
from importlib.metadata import version
from bridge_protocol import Refused, need

ROOT=Path('/c11-paper')
FILES=('hard-stop.sqlite','intents.sqlite','paper-evidence.sqlite',
       'causal-candles.sqlite','observer-native.sqlite','docker-observer.sqlite','observer-native-v4a.sqlite',
       'observer-native-owned-v1.sqlite')

def prove_readonly_startup_lineage(verify, still_owner, pause=time.sleep):
    """Retry only the existing binding proof while still holding the same writer."""
    reason = 'C11_STARTUP_OBSERVER_BINDING'
    prior_refusal = None
    successes = 0
    for attempt in range(5):
        still_owner()
        try:
            verify()
        except Refused as exc:
            if str(exc) != reason:
                raise
            prior_refusal = exc
            successes = 0
        else:
            still_owner()
            successes += 1
            if prior_refusal is None or successes == 2:
                return 'stable' if prior_refusal is None else 'reverified'
        still_owner()
        if attempt < 4:
            pause(0.4)
    if prior_refusal is not None:
        raise prior_refusal
    raise Refused(reason)


def run_locked(scope,mount_identity,State,verify_state_file,intake,config,policy_factory,Worker,*,seed_scope=None,one_shot=None,diagnostic=None,history_startup=None):
    from startup_status import Status,code,first_migration,ARCHIVE,PRESERVED,STATUS,file_info
    def phase(label):
        if diagnostic is not None:diagnostic(label)
    if history_startup is not None:
        from history_factory_admission import FactoryAdmission
        if type(history_startup) is not FactoryAdmission or one_shot is None:
            raise Refused('HISTORY_ONE_SHOT_UNCONSUMED')
    phase('PAPER_WRITER_CHECK')
    with State() as state, ExitStack() as cleanup:
        state.check_locked()
        if seed_scope is not None:
            phase('SEED_SCOPE_CHECK')
            cleanup.callback(seed_scope.activate(state))
        status=Status(ROOT)
        may_write_status=one_shot is None # old untouched entry semantics
        try:
            phase('STATE_FILES_CHECK')
            for name in FILES:
                for suffix in ('','-wal','-shm','-journal'):
                    verify_state_file(ROOT/(name+suffix))
            if one_shot is not None:
                need(seed_scope is not None,'C11_AUTHORIZATION_NEEDS_SAME_WRITER_SCOPE')
                for name in (ARCHIVE,'docker-observer.sqlite',*PRESERVED):
                    path=ROOT/name
                    # Same archived-observer path must already exist. No
                    # first-time observer startup or reinitialization here.
                    need(os.path.lexists(path),'C11_EXISTING_OBSERVER_LINEAGE_REQUIRED')
                    verify_state_file(path)
                # Same lineage validation, twice under the same writer.
                # No status writes and no marker before successful precheck.
                phase('READONLY_LINEAGE_PRECHECK')
                if os.path.lexists(ROOT/STATUS):
                    file_info(ROOT/STATUS,'C11_STARTUP_STATUS_FILE_ID')
                confirmation = prove_readonly_startup_lineage(
                    lambda: first_migration(ROOT,before_consume=True), state.check_locked)
                if confirmation == 'reverified':phase('READONLY_LINEAGE_REVERIFIED')
                state.check_locked()
                phase('ONE_SHOT_CONSUME')
                if history_startup is None:
                    one_shot.consume(state)
                else:
                    from history_startup_receipt import consume_then_publish
                    consume_then_publish(one_shot,state,cleanup,history_startup)
                may_write_status=True
            phase('SCOPED_RELEASE_START')
            status.write('SCOPED_RELEASE_START')
            phase('POST_CONSUME_LINEAGE_CHECK')
            first_migration(ROOT)
            phase('OPEN_AUTHENTIC_SOURCE')
            status.write('ARCHIVE_LINEAGE_ALLOWED')
            if history_startup is not None:history_startup.precheck()
            with intake(mount_identity,scope['generation_floor'],scope['publication_binding']) as (feed,obs):
                first=feed.read()
                need(bool(first),'C11_NO_FRESH_CLASSIFIED_OBSERVATION')
                status.write('PUBLICATION_AND_CLASSIFIER_READY')
                phase('NATIVE_CONFIG')
                cfg=config(scope,[row.pair for row in first])
                status.write('DRY_CONFIG_READY')
                status.write('NATIVE_CONSTRUCTOR_ENTERED')
                phase('NATIVE_WORKER_CONSTRUCTOR')
                if history_startup is not None:history_startup.precheck()
                worker=Worker(cfg,policy_factory(ROOT),feed,obs)
                try:
                    state.check_locked()
                    status.write('NATIVE_WORKER_READY')
                    status.write('NATIVE_RUN_ENTERED')
                    phase('NATIVE_WORKER_RUN')
                    worker.run()
                    status.write('NATIVE_RUN_RETURNED')
                finally:
                    worker.exit()
        except Exception as exc:
            if may_write_status:status.write('REFUSED',code(exc))
            raise

def run_reviewed(scope,mount_identity,release_sha):
    from release_facts import require_release_facts
    from paper_runtime import PaperState, verify_state_file
    from docker_c11_intake import open_intake
    from paper_standalone_entry import startup_config
    from native_worker import PaperWorker
    from standalone_policy import build_policy
    require_release_facts('/opt/c11-release',release_sha,scope)
    run_locked(scope,mount_identity,PaperState,verify_state_file,open_intake,startup_config,build_policy,PaperWorker)

def verify_runtime():
    need(os.getuid()==1000 and os.getgid()==2000 and
         not any(k.startswith('FREQTRADE__') for k in os.environ)
         and not any(Path(n).exists() for n in ('/run/openmarket-credential','/oi20-provider-secret','/var/run/docker.sock')),
         'C11_PAPER_PROCESS_SCOPE')
    need(version('freqtrade')=='2026.10' and version('ccxt')=='4.5.90',
         'C11_RUNTIME_VERSION')

def self_check():
    from freqtrade.configuration.config_validation import validate_config_consistency
    from freqtrade.resolvers import StrategyResolver
    from paper_build_pin import REVIEWED_SCOPE as V3
    from paper_standalone_entry import startup_config
    from observer_host import store_module
    import docker_c11_intake
    import native_worker
    cfg=startup_config(V3,['ERA/USDT:USDT'])
    strategy=StrategyResolver.load_strategy(cfg)
    validate_config_consistency(cfg)
    need(strategy.__class__.__name__=='C11ManagedPaper' and cfg['dry_run'] is True
         and cfg['db_url']=='sqlite:////c11-paper/trades.c11.paper.sqlite','C11_V4_NATIVE_PAPER_VERIFY')
    need(callable(docker_c11_intake.open_intake) and callable(native_worker.PaperBot.observed_ranked)
         and callable(store_module().ObserverStore),'C11_V4_INTAKE_MODULES_PINNED')
    print(json.dumps(dict(status='C11_V4_OCI_OFFLINE_NATIVE_SCHEMA_PASS',strategy='C11ManagedPaper',dry_run=True)),
          flush=True)

def main():
    try:
        verify_runtime()
        os.umask(0o077)
        # Writable Compose/CLI input can never promote this staging image.
        from c11_v4_pin import STAGE_ONLY
        if STAGE_ONLY is False and sys.argv[1:]==['--sealed-artifact-check']:
            from c11_v4_pin import RELEASE_SHA256, REVIEWED_SCOPE, REVIEWED_MOUNT
            from release_facts import require_release_facts
            from docker_observer import binding_for
            need(REVIEWED_SCOPE['publication_binding']==binding_for(REVIEWED_MOUNT),'C11_V4_SEALED_MOUNT')
            require_release_facts('/opt/c11-release',RELEASE_SHA256,REVIEWED_SCOPE)
            self_check()
            return 0
        if STAGE_ONLY is True and sys.argv[1:]==['--isolated-self-check']:
            self_check()
            return 0
        need(STAGE_ONLY is False and len(sys.argv)==1,'C11_V4_STAGE_ONLY_NO_DEPLOYMENT')
        from c11_v4_pin import RELEASE_SHA256, REVIEWED_SCOPE, REVIEWED_MOUNT
        run_reviewed(REVIEWED_SCOPE,REVIEWED_MOUNT,RELEASE_SHA256)
    except Refused as exc:
        from startup_status import code
        print(json.dumps(dict(status=code(exc))),file=sys.stderr,flush=True)
        return 2
    return 0

if __name__=='__main__':raise SystemExit(main())
