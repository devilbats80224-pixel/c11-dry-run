"""Carry original independently pinned release facts into a private build context.

This operation grants no authorization; it preserves the prior facts verbatim.
It deliberately does not copy any files besides the fixed manifest and receipts.
"""
import argparse
import ast
from pathlib import Path
import runpy
import shutil
import stat
from tempfile import TemporaryDirectory


def read_pins(path):
    assignments = {}
    for stmt in ast.parse(path.read_bytes()).body:
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
            name = stmt.targets[0].id
            if name in ("RELEASE_SHA256", "REVIEWED_SCOPE"):
                if name in assignments:
                    raise ValueError("C11_INHERITED_DUPLICATE_PIN")
                assignments[name] = ast.literal_eval(stmt.value)
    return assignments["RELEASE_SHA256"], assignments["REVIEWED_SCOPE"]


def carry(source, destination, pins, verifier):
    digest, scope = read_pins(pins)
    gate = runpy.run_path(str(verifier))
    names = [Path("release.json"), *(Path("receipts") / (route + ".json") for route in gate["ROUTES"])]
    if destination.exists() or destination.is_symlink() or source.is_symlink() or (source / "receipts").is_symlink():
        raise ValueError("C11_REJECTED_INHERITED_RELEASE_PATH")
    for name in names:
        src = source / name
        if not stat.S_ISREG(src.lstat().st_mode):
            raise ValueError("C11_REJECTED_INHERITED_RELEASE_FILE")
    with TemporaryDirectory(prefix=".c11-release-facts-", dir=destination.parent) as temp:
        tmp = Path(temp) / "release"
        (tmp / "receipts").mkdir(parents=True)
        for name in names:
            shutil.copyfile(source / name, tmp / name)
            (tmp / name).chmod(0o444)
        gate["require_release_facts"](tmp, digest, scope)
        tmp.rename(destination)
    return len(names)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "destination", "pinned_scope_py", "release_facts_py"):
        p.add_argument(name, type=Path)
    a = p.parse_args()
    print("C11_INHERITED_RELEASE_FILES_PINNED_COUNT",
          carry(a.source, a.destination, a.pinned_scope_py, a.release_facts_py))


if __name__ == "__main__":
    main()
