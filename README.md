# c11-dry-run
C11 Dry Run CI and reviewed source changes

This initial repository contains only the portable C11 CI bootstrap. It does not
contain the live or simulated account, credentials, historical data, existing
local Git history, production Docker images or replay inputs. GitHub runs syntax
and portable regression checks; full Worker replay is a separate, local gate.
No workflow deploys or merges C11 automatically.
