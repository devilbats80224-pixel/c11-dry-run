"""Verify actual Docker-created launch plans for the exact hosted C11 artifact."""
import json
import re
import runpy
import sys
from pathlib import Path

LAUNCH = runpy.run_path(str(Path(__file__).with_name("check_effective_launch.py")))


def check(row, image, commit, sandbox):
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        return "C11_CI_LAUNCH_UNBOUND_COMMIT"
    inspected = LAUNCH["sanitize_inspect"](row)
    try:
        got = LAUNCH["launch"](inspected)
    except (KeyError, TypeError, ValueError) as exc:
        if str(exc) == "MOUNT_SHADOWS_TESTED_CODE":
            return "C11_CI_LAUNCH_CODE_SHADOW"
        return "C11_CI_LAUNCH_BAD_INSPECT"
    if got[:3] != (image, "python", ("-B", "/opt/c11_owned_modules/release_stage.py")):
        return "C11_CI_LAUNCH_WRONG_IMAGE_OR_ENTRY"
    if got[3] != [("/c11-paper", "bind", sandbox, False)]:
        return "C11_CI_LAUNCH_WRONG_ISOLATED_MOUNT"
    if inspected["StartupEnvironment"] != "pinned" or row["Config"]["User"] != "1000:2000":
        return "C11_CI_LAUNCH_WRONG_EFFECTIVE_ENV_OR_USER"
    if row["HostConfig"]["ReadonlyRootfs"] is not True or row["HostConfig"]["NetworkMode"] != "none":
        return "C11_CI_LAUNCH_NOT_ISOLATED"
    return "C11_CI_LAUNCH_EXACT_IMAGE_ENTRY_AND_NO_SHADOW"


if __name__ == "__main__":
    row, = json.load(sys.stdin)
    result = check(row, *sys.argv[1:])
    print(result)
    raise SystemExit(not result.endswith("_AND_NO_SHADOW"))
