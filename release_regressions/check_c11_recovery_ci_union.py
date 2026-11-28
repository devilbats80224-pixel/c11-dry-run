"""Fail closed if any C11 recovery proof was dropped from combined CI."""
from pathlib import Path
import re
import sys


ROOT=Path(__file__).resolve().parents[1]
STARTUP_REGRESSIONS={
    'test_original_revisit_offer.py',
    'test_original_unwired_retry.py',
    'test_stale_analyzed_cache_revisit.py',
    'test_entry_verdict_production_sink.py',
    'test_retained_minute_cold_prefix.py',
    'test_bootstrap_minute_ranges.py',
    'test_unusable_old_state_bootstrap.py',
    'test_rebuilt_state_continuation.py',
    'test_bounded_official_archive.py',
    'test_advanced_checkpoint_long_gap.py',
    'test_archive_fallback_wire_evidence.py',
    'test_kava_venue_history_alignment.py',
}
WORKER_REPLAYS={'warm-missing-analysis','mixed-blank','mixed-unusable','mixed-retained-blank','mixed-advanced','mixed-retained-thin-classifier','mixed-advanced-direct-conflict',
                'early-roster-return','unseeded-on-demand','archive-source-outage'}


def main():
    shell=(ROOT/'hosted_worker/run-github-replay.sh').read_text()
    workflow=(ROOT/'.github/workflows/c11-ci.yml').read_text()
    lines=re.findall(r'^\s*for test_name in (.*?)\s*;\s*do\s*$',shell,re.M)
    cases=re.findall(r'^\s*case: \[(.*?)\]\s*$',workflow,re.M)
    executed=set(lines[0].split()) if len(lines)==1 else set()
    matrix={x.strip() for x in cases[0].split(',')} if len(cases)==1 else set()
    absent=sorted((STARTUP_REGRESSIONS-executed)|(WORKER_REPLAYS-matrix))
    absent.extend(sorted(t for t in STARTUP_REGRESSIONS if not (ROOT/'hosted_worker'/t).is_file()))
    portable=(ROOT/'release_regressions/test_one_minute_ci_portable.py').read_text()
    gate=(ROOT/'hosted_worker/check_packaged_release_facts.sh').read_text()
    first_current={
        'test_original_unwired_retry.py': 'C11_BUSY_DEBIT_PROBE_RECHECKED_ONLY_WITHIN_ORIGINAL_SCOPE',
        'test_observer_classifier_lease.py': 'C11_FRESH_CLASSIFIER_BEFORE_SHORT_SCOPE_PASS',
        'test_original_revisit_offer.py': 'C11_READABLE_DECISION_CLOCK_DOES_NOT_INHERIT_PRE_WARM_SCOPE',
        'test_stale_analyzed_cache_revisit.py': 'C11_RECEIPT_VISIBLE_BUT_CURRENT_CACHE_HIDES_NATIVE_REVISIT',
        'test_entry_verdict_production_sink.py': 'C11_ORDER_FACT_FIRST_DENIAL_MISSING',
    }
    for fixture,marker in first_current.items():
        path=ROOT/'hosted_worker'/fixture
        if not path.is_file() or marker not in path.read_text():
            absent.append(f'{fixture}:{marker}')
    if 'test_observer_classifier_lease.py' not in portable:
        absent.append('current-rank classifier lease reproduction')
    if ('UNREADABLE_AS_UID_1000' not in gate or 'create_release_facts_layer_context.sh' not in gate):
        absent.append('packaged runtime UID release facts')
    if ('complete_cached_candidates_share_one_ready_owner_pass()' not in
            (ROOT/'hosted_worker/test_retained_minute_cold_prefix.py').read_text()):
        absent.append('same-pass multiple retained prefixes')
    if absent:
        print('C11_REQUIRED_RECOVERY_CI_CASE_MISSING: '+', '.join(sorted(set(absent))))
        return 1
    print('C11_RECOVERY_CI_UNION_REQUIRED_CASES_PRESENT')
    return 0

if __name__=='__main__':
    sys.exit(main())
