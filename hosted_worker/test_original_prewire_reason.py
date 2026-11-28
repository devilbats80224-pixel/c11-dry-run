"""Record only measured exact first refusal on the actual owner queue."""
from concurrent.futures import Future
from types import SimpleNamespace

from dedicated_queue import Job, WorkerOwnedPublicQueue
from shared_native_budget import SharedNativeBudget

E=1_795_876_200_000
GEN=6_750


def scenario(expected):
    now=[E+3500]
    protected=[False]
    valid=[True]
    debit=SharedNativeBudget(lambda:now[0])
    debit.set_scope(GEN,E)
    debit.note_authoritative_ip_header(now[0],30,2400)
    claims=[]
    future=Future()

    def submit(job,token,first_send):
        claims.append(first_send)
        return future

    dummy=SimpleNamespace(exclusive_parallelism=1,submit_original_public=submit,
                          owned_proof_matches=lambda *_:False)
    queue=WorkerOwnedPublicQueue(worker=None,wall=lambda:now[0],
         authority=lambda *_:True,protection_due=lambda:protected[0],executor=dummy,
         global_limiter=debit,handle_attested=lambda *_:True,
         prewire_authority=lambda *_:valid[0])
    queue.rollover(GEN,E)
    ticket=Job('synthetic-1m','SYNTHUSDT',GEN,E,E+21_421,'1m',1)
    assert queue.submit(ticket)
    if expected=="PREWIRE_PROTECTION_HELD":
        protected[0]=True
        valid[0]=False  # Later gate must not overwrite the first refusal.
    elif expected=="PREWIRE_RANK_AUTHORITY_LOST":
        valid[0]=False
    elif expected=="PREWIRE_SCOPE_EXPIRED":
        now[0]=ticket.expires
    elif expected=="PREWIRE_SHARED_METER_REFUSED":
        debit.optional_wire_fault(0,0)
    if expected=="unobserved":
        assert len(claims)==1
        future.set_exception(RuntimeError("synthetic exception before gate"))
    else:
        issued=claims[0]()
        assert issued is (expected is None)
        future.set_result(None)
    protected[0]=False
    assert not queue.poll()
    reason=getattr(queue,'take_prewire_refusal',lambda _:None)(ticket.ticket)
    actual = (now[0],expected) if expected not in (None,"unobserved") else None
    assert reason==actual, ("C11_ZERO_WIRE_RETIRED_WITHOUT_MEASURED_FIRST_REFUSAL", actual, reason)
    assert debit.charged_for_diagnostic(ticket.ticket) is (expected is None)
    assert getattr(queue,'take_prewire_refusal',lambda _:None)(ticket.ticket) is None


for case in ("PREWIRE_PROTECTION_HELD","PREWIRE_RANK_AUTHORITY_LOST","PREWIRE_SCOPE_EXPIRED",
             "PREWIRE_SHARED_METER_REFUSED",None,"unobserved"):
    scenario(case)
print("C11_ORIGINAL_PREWIRE_REASON_IS_FIRST_MEASURED_REFUSAL")

# Existing private startup sink only receives its allowlisted detail schema.
import json
from diagnostic_schema import accepted_symbol_event
from first_gate import FirstGate
packets=[]
gate=FirstGate(packets.append)
gate.start(E)
gate.watch(E+3500,GEN,'SYNTHUSDT',E+21_421)
gate.note(GEN,'SYNTHUSDT','prewire_refusal','PREWIRE_PROTECTION_HELD',E+3501)
gate.note(GEN,'SYNTHUSDT','prewire_refusal','untrusted raw payload must not pass',E+3502)
gate.start(E+60_000)
envelopes=tuple(map(json.loads,packets))
assert all(map(accepted_symbol_event,envelopes))
details=[p for p in envelopes if p.get('kind')=='prewire_refusal']
assert len(details)==1 and details[0]['values']=={'PREWIRE_PROTECTION_HELD':[3501,3501,1]}
print("C11_PREWIRE_REASON_PRIVATE_SINK_SCHEMA_PASS")
