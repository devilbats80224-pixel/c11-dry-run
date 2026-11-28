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
assert actual == expected, f"C11_PRODUCTION_VERDICT_LOG_MISSING expected={expected!r} actual={actual!r}"
assert len(events) == 3, "C11_UNAPPROVED_DIAGNOSTIC_EVENT_REACHED_DOCKER"
print("C11_PACKAGED_PRODUCTION_CALLBACK_VERDICTS_SANITIZED_AND_VISIBLE")
