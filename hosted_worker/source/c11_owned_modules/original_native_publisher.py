"""Owner-only publication of actual Ready commit, no orders or transport."""
from threading import get_ident
from bridge_protocol import Refused
from canonical_guard import same_native_frame
from committed_projection import AuthoritativePrimaryCommit,CommittedProjection,canonical,frame_sha
from coordinator import WIDTH
from native_commit_index import NativeRequest
from no_sqlite_busy import optional_observation
from primary_coverage_gate import NativePrimaryHandoff
from retained_rpd_receipt import RetainedNativeCommitIndex

class NativePublisher:
    def __init__(self,bot,*,wall,authority,accepted_candidate,remaining):
        self.bot=bot
        self.store=bot.policy.bars
        self.wall=wall
        self.authority=authority
        self.accepted_candidate=accepted_candidate
        self.remaining=remaining
        self.owner=get_ident()

    def __call__(self,scope,adapter,batches,frame,callback,*,version=None,version_current=None,multi=None):
        if get_ident()!=self.owner or adapter.ready.store is not self.store or self.store.db.in_transaction:
            return 'HOLD_WRONG_ORIGINAL_PUBLISHER',None
        if hasattr(self.bot,'_c11_native_primary_handoff'):return 'HOLD_ORIGINAL_CALLBACK_ALREADY_OWNED',None
        now=self.wall()
        if not self.authority(scope,now) or not scope.end<=now<scope.expires or self.remaining()<=30:
            return 'HOLD_ORIGINAL_PUBLISHER_LEASE',None
        candidate=self.accepted_candidate(scope)
        if candidate is None or (candidate.pair,candidate.symbol,candidate.generation)!=(scope.pair,scope.symbol,scope.generation):
            return 'HOLD_NO_ORIGINAL_ACCEPTED_RANK',None
        quarter_view=None
        if version is not None:
            from historical_version import VersionView
            if (type(version) is not VersionView or not callable(version_current) or not version_current(scope) or not version.bound(scope,now)
                    or version.quarter_rows is not dict(batches).get(WIDTH['15m'])):
                return 'HOLD_ORIGINAL_PUBLISHER_INPUT',None
            quarter_view=version.quarter_rows
        if multi is not None:
            from multilane_history_view import FrozenReadView
            if type(multi) is not FrozenReadView or not multi.bound(scope,now) or multi.batches is not batches or version is not None:
                return 'HOLD_ORIGINAL_PUBLISHER_INPUT',None
            if (getattr(self.bot,'_owned_revisit_no_positions',False) is not True
                    or getattr(self.bot,'_owned_revisit_reconciled',False) is not True):
                return 'HOLD_ORIGINAL_PUBLISHER_LEASE',None
            quarter_view=multi.quarter_rows
        native=adapter.verified_committed_receipts(scope)
        get_chain=getattr(adapter,'reconfirm_chain',None)
        chain=get_chain(scope) if callable(get_chain) else None
        if chain is not None and chain.batches!=batches:return 'HOLD_NATIVE_RECONFIRM_COMMIT_CONTENT',None
        if not native:return 'HOLD_NO_ORIGINAL_COMMIT_RECEIPTS',None
        idx=RetainedNativeCommitIndex(self.store,lambda _s,t:self.authority(scope,t))
        idx.start(NativeRequest(scope.pair,scope.symbol,scope.generation,scope.end,scope.expires))
        called=False
        try:
            rows=dict(batches)
            before=frame()
            hash_frame=same_native_frame(self.store,scope,before,now,quarter_view=quarter_view)
            if hash_frame is None:return 'HOLD_ANALYZER_NOT_DURABLE_NATIVE_FRAME',None
            if len({float(r.raw[-1][4]).hex() for r,_ in native})!=1:
                return 'HOLD_TERMINAL_NATIVE_CONFLICT',None
            for r,actual_requested in native:
                width=WIDTH[r.ticket.lane]
                if width==300_000:
                    bound=idx.after_original_5m_record(self.store,scope.symbol,r.raw[0][0],scope.end,
                       r.ticket.count,actual_requested,r.received_ms,now,r.raw,rows[width][-r.ticket.count:])
                else:
                    bound=idx.after_original_record(self.store,scope.symbol,width,scope.end,actual_requested,
                         r.received_ms,now,rows[width])
                if bound is not True:return 'HOLD_ORIGINAL_TERMINAL_RECORD_CHANGED',None
            if not self.authority(scope,self.wall()):return 'HOLD_ORIGINAL_PUBLISHER_LEASE',None
            state=optional_observation(lambda:self.store.rpd_progress(scope.symbol,scope.end,self.wall()),(self.store.db,))
            if multi is None:
                lineage=idx.attach_current_rpd(self.store,scope.pair,scope.symbol,scope.end,state,self.wall())
            else:
                lineage=idx.attach_scoped_revision(self.store,scope,state,multi,self.wall())
                state=multi.corrected_rpd() if lineage is not None else None
            if lineage is None:return 'HOLD_NATIVE_RPD_LINEAGE',None
            primary=self.store.recent_primary(scope.symbol,scope.end,self.wall(),scoped_quarters=quarter_view)
            closed=state.closed()
            at=self.wall()
            proof=canonical(tuple((r.source_proof,t) for r,t in native)) if chain is None else chain.proof_sha256
            if version is not None:
                if not version.bound(scope,at):return 'HOLD_ORIGINAL_PUBLISHER_LEASE',None
                proof=canonical((proof,version.commit_proof()))
            if multi is not None:
                if not multi.bound(scope,at):return 'HOLD_ORIGINAL_PUBLISHER_LEASE',None
                proof=canonical((proof,multi.immutable_proof()))
            seal=AuthoritativePrimaryCommit(scope.pair,scope.symbol,scope.generation,scope.end,at,
                   scope.expires,max(r.received_ms for r,_ in native),proof,hash_frame,
                   canonical(primary),canonical(closed),lineage)
            if not idx.index_verified_seal(seal,lambda r,_:r.owner_commit_sha256==proof):
                return 'HOLD_ORIGINAL_PUBLISH_INDEX',None
            projector=CommittedProjection(wall=self.wall,native_owner_lookup=idx.lookup,
                current_authority=lambda _c,_r,t:self.authority(scope,t),
                trusted_rpd_pin=lambda c,e,t:idx.lineage_pin(c.pair,c.symbol,c.generation,e,t))
            event=projector.project(seal,candidate,before,primary,closed)
            gate=NativePrimaryHandoff(wall=self.wall,original_scope=projector.still_native,
                                        frame_sha=lambda _p,e:frame_sha(frame(),e)[0],
                                        scoped_atr=(lambda _p,e:multi.corrected_atr(self.wall())
                                                    if (multi is not None and e==multi.E and multi.bound(scope,self.wall()))
                                                    else None) if multi is not None else None,
                                        scoped_candidate=candidate if multi is not None else None)
            self.bot._c11_native_primary_handoff=gate
            try:
                with gate.committed(event):
                    if gate.resolve(scope.pair,candidate,scope.end) is None:
                        return 'HOLD_ORIGINAL_ANALYZER_CHANGED',None
                    if multi is not None and not (multi.bound(scope,self.wall())
                            and getattr(self.bot,'_owned_revisit_no_positions',False) is True
                            and getattr(self.bot,'_owned_revisit_reconciled',False) is True):
                        return 'HOLD_ORIGINAL_PUBLISHER_LEASE',None
                    if version is not None and not version_current(scope):
                        return 'HOLD_ORIGINAL_PUBLISHER_LEASE',None
                    called=True
                    return 'ORIGINAL_NATIVE_CALLBACK',callback()
            finally:del self.bot._c11_native_primary_handoff
        except (Refused,ValueError,TypeError,IndexError,KeyError,AttributeError):
            if called:raise
            return 'HOLD_ORIGINAL_PUBLISHER_INPUT',None
        finally:idx.stop()
