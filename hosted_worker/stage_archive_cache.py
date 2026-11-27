"""Stage one allowlisted, independently digest-pinned closed public archive set.

A cache supplies raw data only, never a precomputed strategy checkpoint.
Runtime re-verifies this package before use. No transport is opened here.
"""
from hashlib import sha256
import json
from pathlib import Path
import re
import sys
from runpy import run_path

quote_contract_shape = run_path(str(
    Path(__file__).parent / "source/c11_worker/exact_symbol.py"))["quote_contract_shape"]


def copy_archives(source, output):
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
    return {"schema": "c11-reviewed-cold-prefix-v1", "sources": {symbol: {
        "origin_ms": origin, "cutoff_ms": cutoff, "reviewed_at_ms": proof_time,
        "use_until_ms": proof_time + 7 * 86_400_000,
        "contract_sha256": sha256(contract).hexdigest(),
        "manifest_sha256": sha256(manifest_bytes).hexdigest(),
    }}}


if __name__ == "__main__":
    print(json.dumps(copy_archives(sys.argv[1], sys.argv[2]), sort_keys=True))
