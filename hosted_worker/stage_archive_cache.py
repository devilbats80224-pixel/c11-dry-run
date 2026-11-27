"""Stage one allowlisted, independently digest-pinned closed public archive set.

Raw data is the default. A separately validated and explicitly pinned prepared
checkpoint can be included; runtime re-verifies it. No transport is opened here.
"""
from hashlib import sha256
import json
from pathlib import Path
import re
import sys
from runpy import run_path

quote_contract_shape = run_path(str(
    Path(__file__).parent / "source/c11_worker/exact_symbol.py"))["quote_contract_shape"]


def copy_archives(source, output, *, prepared=None, prepared_pin=None):
    source, output = Path(source), Path(output)
    contract = (source / "contract.json").read_bytes()
    payload = json.loads(contract)
    symbol = payload["payload"]["symbol"]
    assert quote_contract_shape(symbol)
    assert payload["source"] == "https://fapi.binance.com/fapi/v1/exchangeInfo"
    origin = payload["payload"]["onboardDate"]
    manifest_bytes = (source / "jct_official_archives/manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    assert 0 < len(manifest) <= 60 and sum(r["bytes"] for r in manifest) <= 26 * 1024 * 1024
    cutoff = json.loads((source / "jct-after-archive.native.json").read_bytes())[0]["start"]
    proof_time = max(payload["actual_received_ms"], *(r["actual_received_ms"] for r in manifest))
    assert origin < cutoff <= proof_time and origin % 900_000 == cutoff % 900_000 == 0
    target = output / symbol
    target.mkdir()
    (target / "contract.json").write_bytes(contract)
    (target / "manifest.json").write_bytes(manifest_bytes)
    seen = set()
    for r in manifest:
        name = r["name"]
        assert re.fullmatch(re.escape(symbol) + r"-1m-\d{4}-\d{2}(?:-\d{2})?\.zip", name)
        assert name not in seen
        seen.add(name)
        path = source / "jct_official_archives" / name
        assert path.is_file() and not path.is_symlink() and path.stat().st_size == r["bytes"]
        data = path.read_bytes()
        digest = sha256(data).hexdigest()
        assert digest == r["sha256"] and len(data) <= 2 * 1024 * 1024
        (target / name).write_bytes(data)
        (target / (name + ".CHECKSUM")).write_text(f"{digest}  {name}\n")
    record = {
        "origin_ms": origin, "cutoff_ms": cutoff, "reviewed_at_ms": proof_time,
        "use_until_ms": proof_time + 7 * 86_400_000,
        "contract_sha256": sha256(contract).hexdigest(),
        "manifest_sha256": sha256(manifest_bytes).hexdigest(),
    }
    if prepared is not None or prepared_pin is not None:
        assert (type(prepared_pin) is str and re.fullmatch(r"[a-f0-9]{64}", prepared_pin)
                and prepared is not None)
        prepared = Path(prepared)
        assert prepared.is_file() and not prepared.is_symlink() and 0 < prepared.stat().st_size <= 256*1024
        raw = prepared.read_bytes()
        assert sha256(raw).hexdigest() == prepared_pin
        body = json.loads(raw)
        state = body["rpd"]["state"]
        assert (body["schema"], body["archive_manifest_sha"], state["symbol"],
                state["origin"], state["next_open"]) == (
                    "c11-reviewed-prepared-prefix-v1", record["manifest_sha256"], symbol, origin, cutoff)
        assert type(state["observed"]) is int and cutoff <= state["observed"] <= proof_time
        (target / "prepared-prefix.json").write_bytes(raw)
        record["prepared_sha256"] = prepared_pin
    return {"schema": "c11-reviewed-cold-prefix-v1", "sources": {symbol: record}}


if __name__ == "__main__":
    print(json.dumps(copy_archives(sys.argv[1], sys.argv[2]), sort_keys=True))
