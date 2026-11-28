"""Fail closed if any C11 recovery proof was dropped from combined CI."""
from pathlib import Path
import re
import sys


ROOT=Path(__file__).resolve().parents[1]
STARTUP_REGRESSIONS={
    'test_original_revisit_offer.py',
    'test_retained_minute_cold_prefix.py',
    'test_bootstrap_minute_ranges.py',
    'test_unusable_old_state_bootstrap.py',
    'test_rebuilt_state_continuation.py',
    'test_bounded_official_archive.py',
    'test_advanced_checkpoint_long_gap.py',
    'test_archive_fallback_wire_evidence.py',
}
WORKER_REPLAYS={'warm-missing-analysis','mixed-blank','mixed-unusable','mixed-retained-blank',
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
    if absent:
        print('C11_REQUIRED_RECOVERY_CI_CASE_MISSING: '+', '.join(sorted(set(absent))))
        return 1
    print('C11_RECOVERY_CI_UNION_REQUIRED_CASES_PRESENT')
    return 0

if __name__=='__main__':
    sys.exit(main())
