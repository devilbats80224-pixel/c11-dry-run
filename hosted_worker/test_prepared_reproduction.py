from verify_prepared_reproduction import compare

assert compare({"count": 2, "known": True, "price": [1.0]},
               {"count": 2, "known": True, "price": [1.0+2e-15]}) == 1
for wrong in ({"count": 3}, {"known": False}, {"price": [1.00001]}, {"price":[float("inf")]}):
    original={"count": 2, "known": True, "price": [1.0]}
    try:
        compare(original,original|wrong)
    except ValueError:
        continue
    raise AssertionError("unexpected change accepted")
print("C11_REVIEWED_REPRODUCTION_CONTROLS_PASS")
