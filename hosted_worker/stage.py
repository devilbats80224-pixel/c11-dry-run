"""Build a source-only, non-activatable C11 replay candidate."""
from hashlib import sha256
import json
from pathlib import Path
import re
import shutil
import sys
from runpy import run_path

copy_archives = run_path(str(Path(__file__).with_name("stage_archive_cache.py")))["copy_archives"]

def dump(path, obj):
    path.write_text(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n")
    return sha256(path.read_bytes()).hexdigest()

source, out = map(Path, sys.argv[1:3])
pinned = sys.argv[3]
assert re.fullmatch("sha256:[0-9a-f]{64}", pinned)
out.mkdir(mode=0o700, parents=False)
tree = out / "opt"
tree.mkdir()
roots = {"c11", "c11_owned_modules", "c11_worker", "c11_background_review", "c11_observer_state_v1"}
files = {}
projection = source.parent.parent / "native_ccxt_projection"
overlay = [(f, Path("c11_owned_modules") / f.name) for f in sorted(projection.glob("*.py"))]
inputs = [(f, f.relative_to(source)) for f in sorted(source.rglob("*.py"))]
assert len(overlay) == 6
for f, rel in inputs + overlay:
    assert not f.is_symlink() and rel.parts[0] in roots and "__pycache__" not in rel.parts
    if rel.as_posix() == "c11_owned_modules/stage_pin.py":
        continue
    target = tree / rel
    target.parent.mkdir(exist_ok=True, parents=True)
    assert not target.exists(), rel
    shutil.copyfile(f, target)
    files["/opt/" + rel.as_posix()] = sha256(target.read_bytes()).hexdigest()
assert len(files) > 150
cold = tree / "c11-reviewed-cold-prefix"
cold.mkdir()
assert len(sys.argv) in (5,6,8)
cold_catalog = (copy_archives(sys.argv[5], cold, prepared=sys.argv[6], prepared_pin=sys.argv[7])
                if len(sys.argv)==8 else copy_archives(sys.argv[5],cold) if len(sys.argv)==6 else {
                    "schema": "c11-reviewed-cold-prefix-v1", "sources": {}
                })
if len(sys.argv)==8:
    symbol,=cold_catalog['sources']
    files[f"/opt/c11-reviewed-cold-prefix/{symbol}/prepared-prefix.json"]=sys.argv[7]
files["/opt/c11-reviewed-cold-prefix/catalog.json"] = dump(cold / "catalog.json", cold_catalog)
seeds = tree / "c11-reviewed-seeds"
seeds.mkdir()
fixtures = Path(sys.argv[4])
for seed in fixtures.glob("*.json"):
    assert re.fullmatch(r"(review|[A-Z0-9]+USDT\.(seed|reproduction))\.json", seed.name)
    path = seeds / seed.name
    shutil.copyfile(seed, path)
    files["/opt/c11-reviewed-seeds/" + seed.name] = sha256(path.read_bytes()).hexdigest()
assert len(list(seeds.iterdir())) == 15
catalog = files["/opt/c11-reviewed-seeds/review.json"]
owner = tree / "c11_owned_modules"
runtime = dump(owner / "release_stage_runtime.json", {
    "release_enabled": False,
    "cold_archive_preparation": "BINANCE_PUBLIC_CLOSED_1M_V1",
    "files": files,
})
(owner / "stage_pin.py").write_text(
    f"RUNTIME_SHA={runtime!r}\nCATALOG_SHA={catalog!r}\nAUTH_SHA=None\n"
    "HISTORY_PREP_ENABLED=True\nDURABLE_REVISIT_ANALYSIS_ENABLED=True\n"
)
(out / "Dockerfile").write_text(f"""FROM docker.io/freqtradeorg/freqtrade@{pinned}
USER 0:0
COPY --chown=0:0 opt /opt
RUN find /opt/c11* -type f -exec chmod 0444 {{}} + && groupadd -f -g 2000 replay
WORKDIR /freqtrade
ENV PYTHONPATH=/freqtrade:/opt/c11_worker:/opt/c11_worker/ports:/opt/c11_worker/sizing:/opt/c11_worker/strategy:/opt/c11:/opt/c11/transport
USER 1000:2000
ENTRYPOINT ["python","-B","/opt/c11_owned_modules/release_stage.py"]
""")
print("DISABLED_CI_IMAGE_STAGE", len(files), runtime)
