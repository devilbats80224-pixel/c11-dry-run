"""Read-only stopped-paper binding guard. This helper cannot start a container."""
import subprocess
import sys
import time
from collections.abc import Callable


def confirm(no_writer: Callable[[], bool], read_exact: Callable[[], bool],
            settle: Callable[[], None]) -> bool:
    """Retry the original proof briefly; fail closed if a writer appears."""
    for attempt in range(3):
        if no_writer() is not True:
            return False
        if read_exact() is True:
            return no_writer() is True
        if attempt < 2:
            settle()
    return False


def main():
    if len(sys.argv)!=3:
        print("C11_STOPPED_BINDING_REQUIRED_PROBES",file=sys.stderr)
        return 2
    no_writer,exact=sys.argv[1:]
    if not confirm(lambda:subprocess.run([no_writer],check=False).returncode==0,
                   lambda:subprocess.run([exact],check=False).returncode==0,
                   lambda:time.sleep(1)):
        print("C11_STOPPED_BINDING_PROOF_REJECTED",file=sys.stderr)
        return 1
    return 0


if __name__=="__main__":
    sys.exit(main())
