"""Compare an independent replay; never change the reference approved bytes."""

import json
import math
from pathlib import Path
import sys

RELATIVE_TOL = 1e-12
ABSOLUTE_TOL = 1e-14

def compare(expected, actual, path="root"):
    if type(expected) is not type(actual):
        raise ValueError(f"{path}: type mismatch")
    if isinstance(expected, dict):
        if expected.keys()!=actual.keys():
            raise ValueError(f"{path}: fields changed")
        return sum(compare(expected[key],actual[key],f"{path}.{key}") for key in sorted(expected))
    if isinstance(expected, list):
        if len(expected)!=len(actual):
            raise ValueError(f"{path}: length changed")
        return sum(compare(want,got,f"{path}[{i}]") for i,(want,got) in enumerate(zip(expected,actual,strict=True)))
    if type(expected) is float:
        if not (math.isfinite(expected) and math.isfinite(actual) and
                math.isclose(expected,actual,rel_tol=RELATIVE_TOL,abs_tol=ABSOLUTE_TOL)):
            raise ValueError(f"{path}: numerical difference exceeds tolerance")
        return expected.hex()!=actual.hex()
    if expected!=actual:
        raise ValueError(f"{path}: exact value changed")
    return 0

if __name__=="__main__":
    try:
        count=compare(*(json.loads(Path(p).read_bytes()) for p in sys.argv[1:]))
    except ValueError as error:
        print(f"::error title=Reviewed C11 checkpoint reproduction::{error}",file=sys.stderr)
        raise SystemExit(1)
    print(f"C11_REVIEWED_REPRODUCTION_MATCH exact_discrete_fields=yes float_roundoff_fields={count}")
