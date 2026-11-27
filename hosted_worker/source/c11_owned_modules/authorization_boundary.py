"""Outbound authorization seam only. Never launches a Worker."""
from hashlib import sha256
from hmac import compare_digest
import re
from binding import load,Hold
HEX=re.compile('[0-9a-f]{64}')
IMAGE=re.compile('sha256:[0-9a-f]{64}')
EVENT=re.compile('Sentinel_[0-9a-f]{32}')
REQUIRED={'schema','verified_owner_event','parent_image','candidate_release_manifest',
          'seed_catalog_sha','paper_ledger','causal_store','accepted_cohort','quarantined','nonce',
          'reviewed_from_ms','reviewed_until_ms','no_real_orders','no_oi_change','redacted_log_mode'}

def validate_baked(data,externally_verified_sha,manifest_sha,catalog_sha,now):
    # Origin is the release-producing owner review (trusted parent), not CLI,
    # config, volume or the observer. This only verifies scope; it is not a grant.
    if externally_verified_sha is None or type(data)is not bytes:return False
    try:
       import stage_pin
       # This identity is baked by the post-merge signer alongside AUTH_SHA.
       # Neither the scope document, runtime configuration nor the host mounts
       # get to choose the reviewed predecessor.
       parent=getattr(stage_pin,'RELEASE_PARENT_IMAGE',None)
       if type(parent) is not str or not IMAGE.fullmatch(parent):return False
       d=load(data,16384)
       return (type(d)is dict and set(d)==REQUIRED and HEX.fullmatch(externally_verified_sha)
         and compare_digest(sha256(data).hexdigest(),externally_verified_sha)
         and d['schema']=='C11_COMPATIBLE_SEVEN_PAPER_OPG_PRIOR_ROLL_FORWARD_V1'
         and bool(EVENT.fullmatch(d['verified_owner_event']))
         and type(d['parent_image']) is str and compare_digest(d['parent_image'],parent)
         and d['candidate_release_manifest']==manifest_sha and d['seed_catalog_sha']==catalog_sha
         and d['paper_ledger']=='/c11-paper/trades.c11.paper.sqlite'
         and d['causal_store']=='/c11-paper/causal-candles.sqlite'
         and d['accepted_cohort']==['BABYUSDT','CYSUSDT','MMTUSDT','OPGUSDT','PHAROSUSDT','RPLUSDT','SAHARAUSDT']
         and d['quarantined']==['AIOTUSDT','LISTAUSDT'] and bool(HEX.fullmatch(d['nonce']))
         and d['no_real_orders'] is d['no_oi_change'] is True and d['redacted_log_mode']=='bounded_docker_local_v1'
         and type(now)is type(d['reviewed_from_ms']) is type(d['reviewed_until_ms']) is int
         and d['reviewed_from_ms']<=now<d['reviewed_until_ms']<=d['reviewed_from_ms']+900_000)
    except (ValueError,TypeError,KeyError):return False
