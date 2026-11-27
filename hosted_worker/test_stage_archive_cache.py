"""Contract identity regression for sealed raw-data packaging, not a Worker proof."""
from hashlib import sha256
import json
from pathlib import Path
import runpy
from tempfile import TemporaryDirectory

copy = runpy.run_path(str(Path(__file__).with_name("stage_archive_cache.py")))["copy_archives"]
window = 900_000

with TemporaryDirectory() as scratch:
    root = Path(scratch)
    for symbol in ("牛来USDT", "龙虾USDT", "../FAKEUSDT"):
        source = root / ("fixture-" + str(len(list(root.iterdir()))))
        source.mkdir()
        original = source / "jct_official_archives"
        original.mkdir()
        output = source / "sealed"
        output.mkdir()
        (source / "contract.json").write_text(json.dumps({
            "source": "https://fapi.binance.com/fapi/v1/exchangeInfo",
            "actual_received_ms": 4 * window,
            "payload": {"symbol": symbol, "onboardDate": window},
        }))
        (source / "jct-after-archive.native.json").write_text(json.dumps([{"start": 2 * window}]))
        case = {"name": symbol + "-1m-2026-11-20.zip", "bytes": 8,
                "sha256": sha256(b"testdata").hexdigest(), "actual_received_ms": 4 * window}
        (original / "manifest.json").write_text(json.dumps([case]))
        if symbol.startswith("../"):
            try:
                copy(source, output)
            except AssertionError:
                continue
            raise AssertionError("UNSAFE_ARCHIVE_SYMBOL_ACCEPTED")
        (original / case["name"]).write_bytes(b"testdata")
        result = copy(source, output)
        assert list(result["sources"]) == [symbol]
        assert (output / symbol / case["name"]).read_bytes() == b"testdata"
print("C11_ARCHIVE_PACKAGE_EXACT_UNICODE_SYMBOL_PASS")
