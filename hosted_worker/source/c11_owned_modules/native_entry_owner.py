"""Build-only entry boundary. Requires genuine original scheduler owner contract."""
from threading import get_ident
from analyzed_frame import exact_analyzed_ohlcv

PROVISIONAL=frozenset(('HOLD_NO_OWNED_SCOPE','HOLD_NOT_READY','HOLD_NO_UNUSED_RECONFIRM_CHAIN',
    'HOLD_NATIVE_MINUTE_NOT_COMMITTED','HOLD_UNPROVEN_ANALYZED_FRAME',
    # Claimed corrected history is never judged via ordinary inputs. This
    # initial refusal keeps exactly one existing same-scope revisit, not a
    # canonical fallback or new permission.
    'HOLD_OPG_REVISION_SCOPED_ANALYZER_REQUIRED'))
QUARTER=900_000
MISSING=object()

def exact_frame_end(frame):
    expected=exact_analyzed_ohlcv(frame)
    end=int(expected.iloc[-1]['date'].value//1_000_000)+QUARTER
    if not 0<end or end%QUARTER:raise ValueError('NATIVE_FRAME_QUARTER_ALIGNMENT')
    return end

class NativeEntryAdviceMixin:
    def analyze_pair(self,pair):
        original=super().analyze_pair
        lane=getattr(self,'_c11_native_entry_owner',None)
        if lane is None or self.process_only_new_candles is not True:
            return original(pair)
        lane.remember_missing_analysis(self,pair)
        if not lane.one_public_revisit(self,pair):
            return original(pair)
        prior=vars(self).get('process_only_new_candles',MISSING)
        self.process_only_new_candles=False
        try:return original(pair)
        finally:
            if prior is MISSING:del self.process_only_new_candles
            else:self.process_only_new_candles=prior
            lane.reset_revisit_frame()

    def advise_indicators(self,dataframe,metadata):
        lane=getattr(self,'_c11_native_entry_owner',None)
        if lane is not None:
            exact=lane.take_revisit_frame(dataframe,metadata)
            if exact is not None:
                # Save exact original input + original indicator method. If the
                # publisher refuses after preparation, native advice restores it.
                lane.fallback_frame=(dataframe.copy(deep=True), super().advise_indicators)
                dataframe=exact
        return super().advise_indicators(dataframe,metadata)

    def advise_entry(self,dataframe,metadata):
        lane=getattr(self,'_c11_native_entry_owner',None)
        original=super().advise_entry
        if lane is None:return original(dataframe,metadata)
        return lane.advise(self,original,dataframe,metadata)

class NativeEntryOwner:
    def __init__(self, *, trusted_owner_input, record, provisional=None, revisit_ready=None,revisit_diagnostic=None,revisit_frame=None,revisit_frame_current=None,discard_revisit_frame=None):
        self.thread=get_ident()
        self.trusted_owner_input=trusted_owner_input
        self.record=record
        self.provisional=provisional
        self.revisit_ready=revisit_ready
        self.revisit_diagnostic=revisit_diagnostic
        self.revisit_frame=revisit_frame
        self.revisit_frame_current=revisit_frame_current
        self.staged_frame=None
        self.fallback_frame=None
        self.discard_revisit_frame=discard_revisit_frame
        self.holds={}
        self.spent=set()

    def remember_missing_analysis(self,strategy,pair):
        # An empty analyzed cache is not a decision frame. The current original
        # OHLCV cache can identify the boundary only; the same-scope scheduler
        # and all existing data / protection checks still own the revisit.
        if (get_ident()!=self.thread or type(pair) is not str or pair in self.holds
                or not callable(self.provisional) or strategy.timeframe!='15m'
                or strategy.config.get('dry_run') is not True):
            return
        try:
            if not strategy.dp.get_analyzed_dataframe(pair,strategy.timeframe)[0].empty:
                return
            frame=strategy.dp.ohlcv(pair,strategy.timeframe,copy=False,candle_type=strategy.config['candle_type_def'])
            end=exact_frame_end(frame)
        except (KeyError,AttributeError,ValueError,TypeError,IndexError):
            return
        self._record_provisional(pair,end,'HOLD_UNPROVEN_ANALYZED_FRAME')

    def one_public_revisit(self,strategy,pair):
        if (get_ident()!=self.thread or type(pair) is not str or not callable(self.revisit_ready)
                or pair not in self.holds or strategy.timeframe!='15m' or strategy.config.get('dry_run') is not True):
            return False
        proof=self.holds[pair]
        key=(pair,proof.end)
        if key in self.spent:return False
        try:
            # This is the public Freqtrade OHLCV cache read used by its own
            # analyze_pair; no network or historical cache-marker edits.
            frame=strategy.dp.ohlcv(pair,strategy.timeframe,copy=False,candle_type=strategy.config['candle_type_def'])
            if exact_frame_end(frame)!=proof.end:
                if self.revisit_diagnostic is not None:self.revisit_diagnostic(proof,'REVISIT_FT_FRAME_END_MISMATCH')
                return False
            if not self.revisit_ready(proof,pair,proof.end):return False
        except (KeyError,AttributeError,ValueError,TypeError,IndexError):
            if self.revisit_diagnostic is not None:self.revisit_diagnostic(proof,'REVISIT_FT_FRAME_UNAVAILABLE')
            return False
        if callable(self.revisit_frame) and callable(self.revisit_frame_current):
            rows=self.revisit_frame(proof,frame)
            if rows is not None:self.staged_frame=(proof,rows)
        self.spent={v for v in self.spent if v[1]>=proof.end}
        self.spent.add(key)
        self.holds.pop(pair,None)
        return True

    def reset_revisit_frame(self):
        self.staged_frame=None
        self.fallback_frame=None
        if callable(self.discard_revisit_frame):self.discard_revisit_frame()

    def take_revisit_frame(self,frame,metadata):
        if get_ident()!=self.thread or self.staged_frame is None:return None
        proof,rows=self.staged_frame
        if metadata.get('pair')!=proof.pair:return None
        self.staged_frame=None
        try:
            if exact_frame_end(frame)!=proof.end or not self.revisit_frame_current(proof):return None
            from pandas import DataFrame,to_datetime
            native=DataFrame(((b.open_ms,b.op,b.high,b.low,b.close,b.volume) for b in rows),
                              columns=('date','open','high','low','close','volume'))
            native['date']=to_datetime(native['date'],unit='ms',utc=True)
            assert exact_frame_end(native)==proof.end
            return native if self.revisit_frame_current(proof) else None
        except (ValueError,TypeError,KeyError,IndexError,OverflowError):return None

    def _record_provisional(self,pair,end,status):
        # Do not erase an earlier original-generation witness on repeated
        # provisional analyses or transfer its one opportunity into a new g.
        self.holds={p:v for p,v in self.holds.items() if v.end>=end}
        self.spent={v for v in self.spent if v[1]>=end}
        if status not in PROVISIONAL:
            self.holds.pop(pair,None)
            self.spent.add((pair,end))
            return
        if (pair,end) in self.spent or pair in self.holds or not callable(self.provisional):return
        proof=self.provisional(pair,end)
        if proof is not None:self.holds[pair]=proof

    def advise(self,strategy,original,dataframe,metadata):
        if get_ident()!=self.thread:raise RuntimeError('ORIGINAL_OWNER_THREAD_REQUIRED')
        pair=metadata.get('pair')
        def no_primary():
            # Only disable extra PRIMARY input for this one entry callback;
            # inherit all normal signals and exit handling. No blind fallback.
            old=strategy.primary_inputs
            strategy.primary_inputs=lambda _pair:None
            try:return original(dataframe,metadata)
            finally:strategy.primary_inputs=old
        try:
            end=exact_frame_end(dataframe)
        except (ValueError,TypeError,AttributeError,IndexError):
            self.record(pair,'HOLD_UNPROVEN_ANALYZED_FRAME')
            self.remember_missing_analysis(strategy,pair)
            return no_primary()
        # Scheduler contract currently intentionally unbound in deployment.
        # It must route to stable original owner receipts, not send/fetch here.
        invoked=False
        original_output=None
        def native_callback():
            nonlocal invoked,original_output
            if invoked:raise RuntimeError('NATIVE_PRIMARY_CALLBACK_ALREADY_INVOKED')
            invoked=True
            original_output=original(dataframe,metadata)
            return original_output
        status,result=self.trusted_owner_input(pair,end,lambda:exact_analyzed_ohlcv(dataframe),native_callback)
        self.record(pair,status)
        if type(pair) is str and callable(self.provisional):self._record_provisional(pair,end,status)
        if invoked:
            if status!='ORIGINAL_NATIVE_CALLBACK' or result is not original_output:
                raise RuntimeError('NATIVE_PRIMARY_RESULT_OWNERSHIP')
            return original_output
        if status=='ORIGINAL_NATIVE_CALLBACK':raise RuntimeError('NATIVE_PRIMARY_CALLBACK_SKIPPED')
        if self.fallback_frame is not None:
            saved,indicators=self.fallback_frame
            self.fallback_frame=None
            # Route-neutral fallback recomputes original indicators from the
            # original *unmodified* provider material, never stale substitutes.
            dataframe=indicators(saved,metadata)
        return no_primary()
