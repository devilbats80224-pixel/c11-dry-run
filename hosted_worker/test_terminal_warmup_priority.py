"""A second ranked candidate's due minute cannot wait for periodic warming."""
import sys
from types import SimpleNamespace
from unittest.mock import patch

sys.path[:0]=["/opt/c11_owned_modules","/opt/c11_worker","/opt/c11_worker/ports"]
from feature_warmup import FeatureWarmup
from native_minute_admission_loop import MinuteAdmissionLoop

def exercise():
    a,b,c=(SimpleNamespace(symbol=s,pair=s+'/USDT:USDT',expires_ns=30) for s in ('A','B','C'))
    requests=[]
    candles=SimpleNamespace(refill_due_terminal_minute=lambda bot,candidate,end: requests.append((candidate.symbol,end)))
    bot=SimpleNamespace(policy=SimpleNamespace(candles=candles),
                        observed_ranked=lambda item:item is not c,
                        _owned_original_cycle_deadline_ns=20)
    service=FeatureWarmup()
    service.next_attempt_ns=999
    terminal=1_795_773_600_000
    # All available ranking rows are passed; buy or protected paths are not used.
    with patch('feature_warmup.time.monotonic_ns',return_value=10), patch(
        'feature_warmup.time.time_ns',return_value=(terminal+1000)*1_000_000):
        hints={(b.pair,b.symbol,terminal),(c.pair,c.symbol,terminal),(a.pair,a.symbol,terminal-900000)}
        service.after_protection(bot,verified_cycle_rows=(a,b,c),due_terminal=hints)
        assert requests==[('B',terminal)],requests
        service.after_protection(bot,verified_cycle_rows=(),due_terminal=hints)
        assert requests==[('B',terminal)],requests
        for size in (10,20):
            roster=tuple(SimpleNamespace(symbol=f'NEW{i}',pair=f'NEW{i}/USDT:USDT',expires_ns=30)
                         for i in range(size))
            hints={(row.pair,row.symbol,terminal) for row in roster}
            requests.clear()
            service.after_protection(bot,verified_cycle_rows=roster,due_terminal=hints)
            assert requests==[(row.symbol,terminal) for row in roster],requests
    duplicate_calls=[]
    def duplicate(*_args):
        duplicate_calls.append(1)
        return ((),(),None),None
    service=FeatureWarmup()
    with patch('feature_warmup.readiness',side_effect=duplicate), patch('feature_warmup.time.monotonic_ns',return_value=10):
        service.after_protection(bot,verified_cycle_rows=(a,b,c))
    assert not duplicate_calls, 'AUTHENTICATED_HISTORY_BYPASSED_SHARED_OWNER'

    class BlankReadOnly:
        def segment(self,_symbol):
            return None
        class db:
            @staticmethod
            def execute(*_args):
                return SimpleNamespace(fetchone=lambda:None)
    for size in (10,20):
        observed=[]
        roster=tuple(SimpleNamespace(symbol=f'BG{i}') for i in range(size))
        owner=SimpleNamespace(
            store=BlankReadOnly(), check_store=lambda:None,
            cold_delivery_window=lambda:True, tap=SimpleNamespace(current=lambda f:f in roster),
            cold_prefix=SimpleNamespace(offer=lambda f:observed.append(f.symbol) or 'QUEUED'))
        loop=MinuteAdmissionLoop(owner,lambda _:None)
        try:
            loop.enroll_cold(roster)
            assert observed==[f.symbol for f in roster],observed
        finally:
            loop.close()
    print('C11_BACKGROUND_BATCH_NO_FIXED_SYMBOL_CAP_AND_SINGLE_NATIVE_OWNER')
    print('C11_SECOND_RANKED_DUE_MINUTE_BYPASSES_BACKGROUND_TIMER_ONLY')

if __name__=='__main__':
    exercise()
