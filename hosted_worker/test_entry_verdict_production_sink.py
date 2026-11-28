"""A packaged native strategy callback must survive the real production redactor."""
import json
import subprocess
import sys

child = r'''
import json
import os
import sys
sys.path[:0] = ["/opt/c11_owned_modules", "/opt/c11_worker", "/opt/c11_worker/ports"]
from pandas import DataFrame, to_datetime
from entry_verdict_receipt import mark_primary, publish
from native_entry_owner import NativeEntryOwner
from retained_diagnostic import redacted_fd2_only
from types import SimpleNamespace as N
from unittest.mock import patch
from standalone_policy import StandalonePolicy
from runtime_facts import NativeEvidence
from c11_native_paper_runner import C11NativePaperRunner

# This is the same FD routing used by release_stage.activate, not the
# warm CI replay's unfiltered stdout and Python list diagnostic sink.
test_error_pipe = os.dup(2)
sink, close = redacted_fd2_only()
try:
    end = 1_795_866_300_000
    stamp = to_datetime(end - 900_000, unit="ms", utc=True)
    df = DataFrame([(stamp, 10., 11., 9., 10., 1.)],
                   columns=("date", "open", "high", "low", "close", "volume"))
    seen = []
    def original(frame, metadata):
        seen.append((frame is df, metadata["pair"]))
        mark_primary("NO_PRIMARY_SIGNAL")
        return frame

    def ready(_pair, _end, frame, invoke):
        assert len(frame()) == 1
        return "ORIGINAL_NATIVE_CALLBACK", invoke()

    owner = NativeEntryOwner(trusted_owner_input=ready, record=lambda *_: None)
    for pair in ("TSTONE/USDT:USDT", "牛来/USDT:USDT"):
        assert owner.advise(object(), original, df, {"pair": pair}) is df
    assert all(exact for exact, _pair in seen) and len(seen) == 2
    assert not sink(b'{"schema":"unapproved","private":"BLOCK_THIS"}\n')
    print("DO_NOT_EXPOSE_PROCESS_STDOUT", flush=True)
    publish("TSTONE/USDT:USDT", end, "order", "UNKNOWN_PRIVATE_REASON")
    invalid = {"schema":"c11-entry-verdict-v1","symbol":"TSTONEUSDT","end_ms":end,
               "phase":"primary","reason":"NO_PRIMARY_SIGNAL","route":"c11_primary","observed_ms":end}
    for bad in (dict(invalid, unapproved="BLOCK_THIS"), dict(invalid, symbol="secret/file"),
                dict(invalid, reason="private runtime text"), dict(invalid, end_ms=end+1),
                dict(invalid, phase="order")):
        assert not sink((json.dumps(bad)+"\n").encode())
    # Call production routing with fake inputs on a disconnected fixture.
    # Each case must return before intent reservation or Exchange.create_order.
    key = "TAGLIKE/USDT:USDT"
    clock = [end+8_608]
    bits = {"primary": None, "atr": None}
    api = N(fapiPublicGetKlines=lambda _: [[end,"10",0,0,0,0,end+59_999,0,1]],
            fapiPublicGetTickerBookTicker=lambda _: {"symbol":"TAGLIKEUSDT","time":clock[0],
                                                   "askPrice":"10.1","askQty":"2"})
    policy = object.__new__(StandalonePolicy)
    policy.slots = N(ready=True)
    policy.bars = N(range=lambda *_: [N(close=10.)])
    evidence = object.__new__(NativeEvidence)
    evidence.policy = policy
    evidence.features = N(read=lambda *_: bits["atr"])
    policy.facts = evidence
    bot = N(config={"dry_run":True, "stake_currency":"USDT"},
            native_paper_exchange=lambda: True,
            exchange=N(_api=api,markets={key:{"id":"TAGLIKEUSDT"}}))
    bot.primary_inputs=lambda _: bits["primary"]
    from native_worker import PaperBot
    bot.policy=policy
    bot.entry_inputs=lambda pair: PaperBot.entry_inputs(bot,pair)
    with patch("time.time_ns", side_effect=lambda: clock[0]*1_000_000):
        assert not C11NativePaperRunner.execute_entry(bot,key,10.,enter_tag="c11_primary")
        bits["primary"] = {"scope":N(end_ms=end,symbol="TAGLIKEUSDT"),"valid_until_ms":end+60_000}
        assert not C11NativePaperRunner.execute_entry(bot,key,10.,enter_tag="c11_primary")
        bits["atr"] = {"observed_at_ms":end+7_980}
        assert not C11NativePaperRunner.execute_entry(bot,key,10.,enter_tag="c11_primary")
    assert bot._entry_open_clock is None
    assert sink(b'{"schema":"c11-reviewed-startup-info-v1","state":"VERIFIED_SYNTHETIC_CALLBACK"}\n')
except Exception:
    # The disposable, data-free fixture reports setup failures to CI,
    # without changing the production redactor or its positive log path.
    os.dup2(test_error_pipe, 2)
    raise
finally:
    os.close(test_error_pipe)
    close()
'''
proc = subprocess.run([sys.executable, "-P", "-B", "-c", child],
                      check=False, capture_output=True, timeout=30)
if proc.returncode != 0:
    print(proc.stderr.decode("utf-8", errors="replace")[-3000:], file=sys.stderr)
    raise AssertionError(f"C11_PRODUCTION_DIAGNOSTIC_PROBE_EXIT actual={proc.returncode}")
assert not proc.stdout, "C11_PRODUCTION_STDOUT_REDACTION_LOST"
events = [json.loads(line) for line in proc.stderr.splitlines()]
assert any(x.get("state") == "VERIFIED_SYNTHETIC_CALLBACK" for x in events), "C11_TEST_MISSING_CALLBACK_PROOF"
actual = [(x["symbol"], x["phase"], x["reason"]) for x in events if x["schema"] == "c11-entry-verdict-v1"]
expected = [("TSTONEUSDT", "primary", "NO_PRIMARY_SIGNAL"), ("牛来USDT", "primary", "NO_PRIMARY_SIGNAL")]
assert actual[:2] == expected, f"C11_PRODUCTION_VERDICT_LOG_MISSING expected={expected!r} actual={actual!r}"
order_expected = [("TAGLIKEUSDT", "order", stage) for stage in
                  ("ENTRY_FACTS_PRIMARY_RECHECK_UNAVAILABLE", "ENTRY_FACTS_ATR_UNAVAILABLE",
                   "ENTRY_FACTS_EXECUTABLE_SIDE_MISMATCH")]
assert actual[2:] == order_expected, f"C11_ORDER_FACT_FIRST_DENIAL_MISSING expected={order_expected!r} actual={actual[2:]!r}"
assert len(events) == 6, "C11_UNAPPROVED_DIAGNOSTIC_EVENT_REACHED_DOCKER"
print("C11_PACKAGED_PRODUCTION_CALLBACK_VERDICTS_SANITIZED_AND_VISIBLE")
