# c11-dry-run
C11 Dry Run CI and reviewed source changes

This initial repository contains only the portable C11 CI bootstrap. It does not
contain the live or simulated account, credentials, historical data, existing
local Git history, production Docker images or replay inputs. GitHub runs syntax
and portable regression checks; full Worker replay is a separate, local gate.
No workflow deploys or merges C11 automatically.

The required PR check distinguishes workflow/docs-only changes from runtime,
replay-harness and packaging changes. The latter fail closed until a trusted
native replay check is connected to the PR; a green portable suite is not a
substitute. No private fixtures or self-hosted runner are exposed to public PRs.

Native replay must verify the exact packaged Worker. In particular, retain every
admitted closed 1m candle and its derived bars atomically, reject conflicting
raw rows without partial writes, keep 5m/15m off the wire, and prove the original
strategy receives the candidate in the intended window. Include partial, absent,
and stale checkpoints; do not substitute a component test for full replay.
