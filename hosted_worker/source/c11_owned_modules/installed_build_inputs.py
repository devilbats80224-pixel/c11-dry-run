"""Exact installed C11 public-only constructor. Does not start a Worker."""
from exact_symbol import quote_contract_shape
from importlib.metadata import version
from threading import get_ident
from time import monotonic_ns, time_ns
from urllib.parse import urlsplit

from ccxt import binance
from requests import Session
from requests.adapters import HTTPAdapter
from bridge_protocol import Refused
from rpd_checkpoint import RpdState
from sqlite3 import OperationalError
from no_sqlite_busy import optional_observation
from native_site_callbacks import install_native_meter
from native_candles import NativeCandles
from c11_paper_exchange import C11PaperExchange
from public_only import guard_client, check_request, CCXT_KEYS
from compiled_candidate_factory import BuildInputs, compiled_factory

BINANCE_WEIGHT_LIMIT = 2400
OPTIONAL_TIMEOUT_MS = 900

class KlineOnlySession(Session):
    """No ambient credentials, redirects, or automatic retry path."""
    def __init__(self):
        super().__init__()
        self.trust_env=False
        self.mount('https://',HTTPAdapter(max_retries=0,pool_connections=1,pool_maxsize=1))
        self.mount('http://',HTTPAdapter(max_retries=0,pool_connections=1,pool_maxsize=1))
    def request(self,method,url,**kwargs):
        check_request(str(url),method,kwargs.get('headers'),kwargs.get('data'))
        if (urlsplit(str(url)).path!='/fapi/v1/klines' or
                kwargs.get('auth') is not None or kwargs.get('cert') is not None or
                kwargs.get('cookies') or kwargs.get('proxies') or kwargs.get('params')):
            raise ValueError('C11_OPTIONAL_EXACT_KLINE_SESSION')
        kwargs.update(allow_redirects=False,verify=True,timeout=(.45,.45))
        return super().request(method,url,**kwargs)

def installed_build_inputs(worker,*,native_entry_deadline_ns,record,
                           verified_limit=lambda:BINANCE_WEIGHT_LIMIT):
    """All three local owner callbacks must be supplied by the reviewed release."""
    b=worker.freqtrade
    owner=get_ident()
    if (type(b.exchange) is not C11PaperExchange or not b.native_paper_exchange() or
            b.config.get('dry_run') is not True or version('freqtrade')!='2026.10' or
            version('ccxt')!='4.5.90'):
        raise ValueError('C11_EXACT_INSTALLED_SITE_REQUIRED')
    handle=[None]
    wall=lambda:time_ns()//1_000_000

    def make_unsigned(w):
        if w is not worker or get_ident()!=owner or handle[0] is not None:
            raise ValueError('C11_SINGLE_WORKER_PUBLIC_CLIENT_ONLY')
        session=KlineOnlySession()
        client=binance({'session':session,'enableRateLimit':False,'timeout':OPTIONAL_TIMEOUT_MS,
                        'verify':True,'maxRetriesOnFailure':0,'options':{'defaultType':'future','maxRetriesOnFailure':0}})
        if type(client.session) is not KlineOnlySession or client.session is not session:
            session.close()
            raise ValueError('C11_INSTALLED_CCXT_SESSION_MISMATCH')
        handle[0]=guard_client(client)
        return handle[0]

    def attest(c):
        return (c is handle[0] and type(c) is binance and type(c.session) is KlineOnlySession and
                c.session.trust_env is False and c.verify is True and c.enableRateLimit is False and
                c.timeout==OPTIONAL_TIMEOUT_MS and not any(getattr(c,k,None) for k in CCXT_KEYS))

    def remaining():
        # Scheduler supplies original monotonic protection deadline, not a
        # guessed delay from a market receipt or a renewed per-candidate lease.
        if get_ident()!=owner:return 0
        now=monotonic_ns()
        limit=native_entry_deadline_ns()
        return max(0,(limit-now)//1_000_000) if type(limit) is int else 0

    def due():
        # The transport reads only the atomic monotonic cutoff, not SQLite
        # or any mutable owner state. Its independent fence also closes
        # before protected work and holds on session change.
        cutoff=native_entry_deadline_ns()
        return type(cutoff) is not int or cutoff-monotonic_ns()<=30_000_000

    def checkpoint(bot,row,end):
        if bot is not b or get_ident()!=owner:return None
        try:
            market=NativeCandles.market(bot,row.pair,row.symbol)
            launch=market['info'].get('onboardDate')
            if type(launch) is not int or not 0<launch<end or launch%300_000:return None
            bars=bot.policy.bars
            now=wall()
            def read():
                proof=bars.contract_segment(row.symbol,launch,now)
                if proof is None:return None
                prior=bars.db.execute('SELECT payload FROM rpd_checkpoint WHERE symbol=?',(row.symbol,)).fetchone()
                if prior is None:return None
                state=RpdState.restore(prior[0])
                if (state.symbol,state.origin)!=(row.symbol,proof.open_ms) or state.observed>now:return None
                cursor=state.next_open
                return cursor if type(cursor) is int and proof.open_ms<=cursor<=end<=now and cursor%300_000==0 else None
            value=optional_observation(read,(bars.db,))
            return value if type(value) is int else None
        except (OperationalError,Refused,KeyError,TypeError,ValueError):
            return None

    return BuildInputs(wall,monotonic_ns,make_unsigned,check_request,verified_limit,
        lambda w,m:install_native_meter(w,m,C11PaperExchange) if w is worker else None,
        attest,checkpoint,due,remaining,record)

def installed_site_factory(seal,record,*,diagnostic_sink=None,prior_seed_factory=None,history_review=None,history_startup=None,prepared_history_quarantine=None,archive_prep=None,reviewed_revision=None):
    """Build-only seam; no worker binding unless called by the exact owner."""
    prepared=prepared_history_quarantine is not None
    if archive_prep is not None:
        from archive_prep_lane import ArchivePrepLane
        if not prepared or type(archive_prep) is not ArchivePrepLane:
            raise ValueError('C11_ARCHIVE_PREP_UNARMED_OR_TYPE')
    if prepared:
        from stage_pin import HISTORY_PREP_ENABLED
        if (HISTORY_PREP_ENABLED is not True or history_startup is None or history_review is not None
                or type(prepared_history_quarantine) is not tuple
                or len(set(prepared_history_quarantine))!=len(prepared_history_quarantine)
                or not all(quote_contract_shape(x)
                           for x in prepared_history_quarantine)):
            raise ValueError('C11_PREPARED_HISTORY_UNARMED_OR_SCOPE_INVALID')
    if reviewed_revision is not None:
        from reviewed_opg_selector import ReviewedOpgSource
        if type(reviewed_revision) is not ReviewedOpgSource or history_startup is None:
            raise ValueError('OPG_REVISION_EXACT_LOCAL_STARTUP_REQUIRED')
    def bind(worker):
        if history_startup is not None:
            from history_factory_admission import FactoryAdmission
            if type(history_startup) is not FactoryAdmission:
                raise ValueError('HISTORY_EXACT_LOCAL_STARTUP_REQUIRED')
            # Validate before constructing the native binding; a normal deferred
            # startup (no accepted OI roster yet) must not consume this receipt.
            history_startup.precheck()
        bot=worker.freqtrade
        inputs=installed_build_inputs(worker,
                native_entry_deadline_ns=lambda:getattr(bot,'_owned_original_cycle_deadline_ns',None),
                record=record)
        binding=compiled_factory(inputs,seal,
               startup_meter=getattr(worker,'_c11_bootstrap_rate',None),diagnostic_sink=diagnostic_sink,prior_seed_factory=prior_seed_factory,history_review=history_review,prepared_history_review=prepared,reviewed_revision=reviewed_revision)(worker)
        if history_startup is not None:
            try:
                receipt=history_startup.claim()
                from owner_bound_history import BoundOwnerHistory
                run=binding[0].scheduler
                if run.isolated_history is not None:
                    raise ValueError('C11_HISTORY_MULTIPLE_OWNER_BINDINGS')
                if prepared:
                    from prepared_owner_history import BoundOwnerHistory as CurrentOwnerHistory
                    revision=run.commit_and_decide.multilane_prepare
                    run.isolated_history=CurrentOwnerHistory(run,receipt,verified_quarantine=prepared_history_quarantine,
                        archive_prep=archive_prep,reviewed_opg_selector=revision,diagnostic_sink=diagnostic_sink)
                else:
                    run.isolated_history=BoundOwnerHistory(run,receipt)
            except BaseException:
                binding[0].scheduler.stop_and_join()
                raise
        return binding
    return bind
