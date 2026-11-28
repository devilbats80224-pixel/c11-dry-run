"""Synthetic release evidence only; never invokes the worker or opens a ledger."""
import argparse
from hashlib import sha256
import json
from pathlib import Path
import runpy
import sys
from unittest.mock import patch


def sample(destination, pin_file, production_pin, facts_file, mode):
    release_pins = runpy.run_path(str(production_pin))
    scope = {**release_pins["REVIEWED_SCOPE"], "release_id": "c11-paper-ci-fixture"}
    routes = {}
    (destination / "receipts").mkdir(parents=True)
    for route in runpy.run_path(str(facts_file))["ROUTES"]:
        receipt = json.dumps({"fixture_only": True, "case": route}).encode()
        (destination / "receipts" / (route + ".json")).write_bytes(receipt)
        routes[route] = {"disposition": "PINNED_PAPER_FIXTURE_EVIDENCE",
                         "sha256": sha256(receipt).hexdigest()}
    raw = json.dumps({"version": 1, "scope": scope, "routes": routes}, sort_keys=True).encode()
    (destination / "release.json").write_bytes(raw)
    pin_file.write_text(f"RELEASE_SHA256={sha256(raw).hexdigest()!r}\nREVIEWED_SCOPE={scope!r}\n")
    if mode == "missing":
        (destination / "release.json").unlink()
    elif mode == "missing-route":
        (destination / "receipts" / (next(iter(routes)) + ".json")).unlink()
    elif mode == "tampered":
        (destination / "receipts" / (next(iter(routes)) + ".json")).write_bytes(b"changed")


def check_in_image(pins, expectation):
    sys.path.insert(0, "/opt/c11_owned_modules")
    import c11_v4_pin
    from release_facts import ReleaseRefused, ROUTES
    from release_stage import verify_activation_assets

    synthetic = runpy.run_path(str(pins))
    # Override constants only in the in-process regression. The installed
    # code, real read paths, permission/versions and immutable filesystem
    # are the same as the shipped candidate. No new activation authority.
    assert synthetic["REVIEWED_SCOPE"]["release_id"] == "c11-paper-ci-fixture"
    with patch.object(c11_v4_pin, "RELEASE_SHA256", synthetic["RELEASE_SHA256"]), patch.object(
        c11_v4_pin, "REVIEWED_SCOPE", synthetic["REVIEWED_SCOPE"]
    ):
        try:
            actual = verify_activation_assets()
        except ReleaseRefused as exc:
            expected_reason = "C11_RELEASE_FILE_MISSING" if expectation == "UNREADABLE_AS_UID_1000" else expectation
            assert str(exc) == expected_reason, ("C11_PACKAGED_RELEASE_FACTS", expectation, str(exc))
            if expectation == "UNREADABLE_AS_UID_1000":
                assert isinstance(exc.__cause__, PermissionError), (
                    "C11_PACKAGED_RELEASE_ASSET_MODE", "PermissionError", type(exc.__cause__).__name__
                )
        else:
            assert expectation == "PASS" and tuple(actual) == tuple(ROUTES)
    print("C11_PACKAGED_RELEASE_FACTS", expectation)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "check"))
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args()
    if args.mode == "prepare":
        sample(*(Path(s) for s in args.paths[:4]), args.paths[4])
    else:
        check_in_image(Path(args.paths[0]), args.paths[1])
