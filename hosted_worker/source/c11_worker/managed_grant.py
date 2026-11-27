"""Trust boundary implemented by the formal managed launcher."""


def host_grant():
    from bridge_protocol import Refused
    raise Refused('C11_MANAGED_HOST_DISPATCH_UNAVAILABLE')


def runtime_grant():
    from bridge_protocol import Refused
    raise Refused('C11_MANAGED_RUNTIME_VERIFIER_UNAVAILABLE')
