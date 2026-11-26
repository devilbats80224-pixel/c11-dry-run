# c11-dry-run
C11 Dry Run CI and reviewed source changes

This initial repository contains only the portable C11 CI bootstrap. It does not
contain the live or simulated account, credentials, historical data, existing
local Git history, production Docker images or replay inputs. GitHub runs syntax
and portable regression checks; full Worker replay is a separate, local gate.
No workflow deploys or merges C11 automatically.

The required PR check distinguishes workflow/docs-only changes from runtime,
replay-harness and packaging changes. The latter require every native status from the trusted reporter on the exact
PR merge commit. Missing, failed and stale results cannot approve a merge. No private fixtures or self-hosted runner are exposed to public PRs.

Native replay must verify the exact packaged Worker. In particular, retain every
admitted closed 1m candle and its derived bars atomically, reject conflicting
raw rows without partial writes, keep 5m/15m off the wire, and prove the original
strategy receives the candidate in the intended window. Include partial, absent,
and stale checkpoints; do not substitute a component test for full replay.

Keep known failure modes in `release_regressions/required_cases.json`. The
required PR job checks that existing cases have not been removed or changed
and verifies that each portable case actually passed (a skipped or undiscovered
test does not count). Add the regression with a fix, and keep the old-fails /
new-passes evidence. Native cases remain required but cannot pass from this
public workflow; the runtime merge rule stays closed unless the trusted Mac reporter
posts a pass for every required case on the exact proposed merge commit.

GitHub also runs the official Freqtrade 2026.10 Docker image, pinned by its
Linux/amd64 manifest digest. A network-disabled fixture feeds synthetic
closed 1m data through real Freqtrade DataProvider and strategy analysis,
with blank, partial, and stale starting data, plus deliberate failure controls.
This tests the public integration contract, not the private C11 code, private
strategy, exact deployment image, or original paper ledger. Runtime changes
still require the separately verified Mac results.

Regression-first: record every newly reproduced defect in the required
registry and verify the old failure before starting another fix branch.
Use the exact same failing case to verify the fix. A no-wire failure must not
be reported as success merely because there were no direct 5m/15m requests.

Before publishing, stage only reviewed source and synthetic fixtures.
`.gitignore` protects untracked files and CI also detects ignored paths
that were already tracked or force-added. It does not purge published Git
history or identify sensitive information embedded in a normally named source
file, so review the staged diff before pushing.

The same offline run injects a synthetic OI20 publication into the current
rank-slot and symbol boundary before native Freqtrade. It checks preserved
rank and original Unicode identity, malformed row isolation, the 20% edge,
and expired or malformed-frame refusal. This does not start or replace the
production OI20 publisher and no OpenMarket key is present on the runner.
