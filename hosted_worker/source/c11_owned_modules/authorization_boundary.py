"""Outbound authorization seam only. Never launches a Worker."""
from hashlib import sha256
from hmac import compare_digest
import re
from binding import load

HEX=re.compile('[0-9a-f]{64}')
IMAGE=re.compile('sha256:[0-9a-f]{64}')
EVENT=re.compile('Sentinel_[0-9a-f]{32}')
REQUIRED={'schema','verified_owner_event','parent_image','candidate_release_manifest',
          'seed_catalog_sha','paper_ledger','causal_store','accepted_cohort','quarantined','nonce',
          'reviewed_from_ms','reviewed_until_ms','no_real_orders','no_oi_change','redacted_log_mode'}


def denial_reason(data,externally_verified_sha,manifest_sha,catalog_sha,now):
    # The release-producing owner seals both identities into the image,
    # never a runtime environment variable, volume, or CLI option.
    if type(externally_verified_sha) is not str or type(data) is not bytes:
        return 'MISSING_BAKED_SCOPE'
    try:
        import stage_pin
        parent=getattr(stage_pin,'RELEASE_PARENT_IMAGE',None)
        if type(parent) is not str or not IMAGE.fullmatch(parent):
            return 'MISSING_REVIEWED_PREDECESSOR'
        if not HEX.fullmatch(externally_verified_sha) or not compare_digest(sha256(data).hexdigest(),externally_verified_sha):
            return 'BAKED_SCOPE_HASH_MISMATCH'
        d=load(data,16384)
        if type(d) is not dict or set(d)!=REQUIRED or d['schema']!='C11_COMPATIBLE_SEVEN_PAPER_OPG_PRIOR_ROLL_FORWARD_V1':
            return 'BAKED_SCOPE_SCHEMA_MISMATCH'
        if type(d['parent_image']) is not str or not compare_digest(d['parent_image'],parent):
            return 'REVIEWED_PREDECESSOR_MISMATCH'
        if not EVENT.fullmatch(d['verified_owner_event']) or not HEX.fullmatch(d['nonce']):
            return 'INVALID_OWNER_REVIEW_IDENTITY'
        if d['candidate_release_manifest']!=manifest_sha or d['seed_catalog_sha']!=catalog_sha:
            return 'RUNTIME_OR_CATALOG_MISMATCH'
        if d['paper_ledger']!='/c11-paper/trades.c11.paper.sqlite' or d['causal_store']!='/c11-paper/causal-candles.sqlite':
            return 'PAPER_PATH_MISMATCH'
        if (d['accepted_cohort']!=['BABYUSDT','CYSUSDT','MMTUSDT','OPGUSDT','PHAROSUSDT','RPLUSDT','SAHARAUSDT']
                or d['quarantined']!=['AIOTUSDT','LISTAUSDT']):
            return 'ACCEPTED_SCOPE_LIST_MISMATCH'
        if d['no_real_orders'] is not True or d['no_oi_change'] is not True or d['redacted_log_mode']!='bounded_docker_local_v1':
            return 'PAPER_ONLY_CONSTRAINT_MISMATCH'
        start,stop=d['reviewed_from_ms'],d['reviewed_until_ms']
        if not (type(now) is type(start) is type(stop) is int and start<=now<stop<=start+900_000):
            return 'BAKED_SCOPE_EXPIRED_OR_INVALID'
    except (ValueError,TypeError,KeyError):
        return 'BAKED_SCOPE_DATA_INVALID'
    return None


def validate_baked(data,externally_verified_sha,manifest_sha,catalog_sha,now):
    # This verifies a scope. The existing OneShot and writer lock always
    # establish actual process-local authority.
    return denial_reason(data,externally_verified_sha,manifest_sha,catalog_sha,now) is None
