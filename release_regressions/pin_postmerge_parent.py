"""Bind a post-merge paper release to the independently verified predecessor."""
import argparse
import ast
from hashlib import sha256
from hmac import compare_digest
import json
import os
from pathlib import Path
import re
from tempfile import NamedTemporaryFile

IMAGE = re.compile(r"sha256:[0-9a-f]{64}")
DIGEST = re.compile(r"[0-9a-f]{64}")


def bind_scope(stage_text, scope, observed_parent):
    if not IMAGE.fullmatch(observed_parent):
        raise ValueError("C11_INVALID_OBSERVED_DB_OWNER_IMAGE")
    pins = {}
    for stmt in ast.parse(stage_text).body:
        if not isinstance(stmt, ast.Assign) or len(stmt.targets) != 1 or not isinstance(stmt.targets[0], ast.Name):
            raise ValueError("C11_INVALID_STAGE_PINS")
        key = stmt.targets[0].id
        if key in pins:
            raise ValueError("C11_DUPLICATE_STAGE_PIN")
        pins[key] = ast.literal_eval(stmt.value)
    auth = pins.get("AUTH_SHA")
    if not isinstance(auth, str) or not DIGEST.fullmatch(auth) or not compare_digest(sha256(scope).hexdigest(), auth):
        raise ValueError("C11_POSTMERGE_APPROVAL_PIN_MISMATCH")
    data = json.loads(scope)
    if (not isinstance(data, dict) or data.get("parent_image") != observed_parent
            or data.get("candidate_release_manifest") != pins.get("RUNTIME_SHA")
            or data.get("seed_catalog_sha") != pins.get("CATALOG_SHA")
            or data.get("no_real_orders") is not True or data.get("no_oi_change") is not True):
        raise ValueError("C11_POSTMERGE_REVIEWED_PARENT_OR_SCOPE_MISMATCH")
    if pins.get("RELEASE_PARENT_IMAGE") not in (None, observed_parent):
        raise ValueError("C11_STAGE_PARENT_ALREADY_PINNED")
    pins["RELEASE_PARENT_IMAGE"] = observed_parent
    return "".join(f"{key}={value!r}\n" for key, value in pins.items())


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("stage_pin", type=Path)
    p.add_argument("scope", type=Path)
    p.add_argument("verified_parent")
    a = p.parse_args()
    content = bind_scope(a.stage_pin.read_text(), a.scope.read_bytes(), a.verified_parent)
    with NamedTemporaryFile(dir=a.stage_pin.parent, prefix=".parent-pin-", delete=False) as out:
        temporary = Path(out.name)
        out.write(content.encode())
    try:
        os.replace(temporary, a.stage_pin)
    finally:
        temporary.unlink(missing_ok=True)
    print("C11_POSTMERGE_REVIEWED_PARENT_PINNED")


if __name__ == "__main__":
    main()
