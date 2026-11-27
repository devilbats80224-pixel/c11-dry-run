"""Gate for an independently released, isolated C11 PAPER Worker.

The immutable launcher must supply its reviewed release digest and actual scope.
Never source either from Compose env, CLI or the receipt being verified.
This gate is not a platform execution approval or runtime grant.
"""
import hashlib
import hmac
import json
import math
from pathlib import Path
import re

HEX = re.compile(r"[0-9a-f]{64}\Z")
ROUTES = (
    "primary_nw15_and_nw5_and_sweep",
    "hard_pending_prepriority",
    "settled_partial_and_restart",
    "atr_er_seed_and_restart",
    "holding_weak_mature_early2_contour",
    "incumbent_slot_and_public_quote",
    "authenticated_publication_observer",
    "public_only_and_paper_ledger_isolation",
)
SCOPE = {
    "mode", "release_id", "base_image", "policy_tree_sha256",
    "wallet_usdt", "publication_binding", "generation_floor",
    "ledger", "egress", "compose_config_sha256",
}


class ReleaseRefused(RuntimeError):
    pass


def require(ok, code):
    if not ok:
        raise ReleaseRefused(code)


def pairs(items):
    out = {}
    for key, value in items:
        require(key not in out, "C11_RELEASE_JSON_DUPLICATE")
        out[key] = value
    return out


def bad_number(_):
    raise ReleaseRefused("C11_RELEASE_JSON_NUMBER")


def read(path, limit):
    try:
        with path.open("rb") as f:
            data = f.read(limit + 1)
    except OSError as err:
        raise ReleaseRefused("C11_RELEASE_FILE_MISSING") from err
    require(0 < len(data) <= limit, "C11_RELEASE_FILE_SIZE")
    return data


def digest(b):
    return hashlib.sha256(b).hexdigest()


def sha(x):
    return type(x) is str and HEX.fullmatch(x) is not None


def require_release_facts(root, trusted_release_sha, actual_scope):
    """Return admitted evidence IDs; never create a worker or synthesize facts."""
    require(sha(trusted_release_sha), "C11_RELEASE_INDEPENDENT_PIN_REQUIRED")
    require(type(actual_scope) is dict and set(actual_scope) == SCOPE,
            "C11_RELEASE_SCOPE_SCHEMA")
    s = actual_scope
    require(
        s["mode"] == "C11_PAPER_ONLY" and s["ledger"] == "/c11-paper/trades.c11.paper.sqlite"
        and s["egress"] == "public-binance-fapi-get-only"
        and type(s["release_id"]) is str and re.fullmatch(r"c11-paper-[a-z0-9-]{1,48}", s["release_id"])
        and type(s["base_image"]) is str and s["base_image"].startswith("sha256:")
        and sha(s["base_image"][7:])
        and all(sha(s[k]) for k in ("publication_binding", "policy_tree_sha256", "compose_config_sha256"))
        and type(s["wallet_usdt"]) in (int, float) and 0 < s["wallet_usdt"] <= 1e12
        and math.isfinite(s["wallet_usdt"])
        and type(s["generation_floor"]) is int and s["generation_floor"] > 0,
        "C11_RELEASE_PAPER_SCOPE",
    )
    root = Path(root)
    data = read(root / "release.json", 65536)
    require(hmac.compare_digest(digest(data), trusted_release_sha),
            "C11_RELEASE_PIN_MISMATCH")
    try:
        bundle = json.loads(data, object_pairs_hook=pairs, parse_constant=bad_number)
    except (ValueError, UnicodeError) as exc:
        raise ReleaseRefused("C11_RELEASE_JSON_INVALID") from exc
    require(type(bundle) is dict and set(bundle) == {"version", "scope", "routes"},
            "C11_RELEASE_BUNDLE_SCHEMA")
    require(type(bundle["version"]) is int and bundle["version"] == 1,
            "C11_RELEASE_VERSION")
    require(type(bundle["scope"]) is dict and set(bundle["scope"]) == SCOPE
            and all(type(bundle["scope"][k]) is type(s[k]) and bundle["scope"][k] == s[k] for k in SCOPE),
            "C11_RELEASE_BINDING_MISMATCH")
    routes = bundle["routes"]
    require(type(routes) is dict and set(routes) == set(ROUTES), "C11_RELEASE_INCOMPLETE_ROUTES")
    for name in ROUTES:
        evidence = routes[name]
        require(type(evidence) is dict and set(evidence) == {"disposition", "sha256"}
                and evidence["disposition"] == "PINNED_PAPER_FIXTURE_EVIDENCE"
                and sha(evidence["sha256"]), "C11_RELEASE_ROUTE_NOT_ACCEPTED")
        # Fixed routes imply fixed names; the manifest cannot choose any path.
        raw = read(root / "receipts" / (name + ".json"), 8 * 1024 * 1024)
        require(hmac.compare_digest(digest(raw), evidence["sha256"]), "C11_RELEASE_EVIDENCE_MISMATCH")
    return tuple(ROUTES)
