"""Drop-in reader/observer pair; call only within C11 PaperState's single writer."""
from contextlib import contextmanager
from pathlib import Path
import secrets
from bridge_protocol import need
from docker_observer import DockerObserver, CaptureBridge, binding_for
from docker_publication import Publication
from entry_feed import EntryFeed
from observer_rpc import ObserverClient

@contextmanager
def open_intake(mount_identity, generation_floor, publication_binding, *, wall=None, mono=None):
    """All arguments are baked release pins, never environment or provider input."""
    need(binding_for(mount_identity)==publication_binding,'C11_DOCKER_RELEASE_PUBLICATION_BINDING')
    pub=Publication(Path('/oi20-docker-publication'),501,2000,generation_floor,wall,mono,
                    expected_mount=mount_identity,require_readonly=True)
    try:
        obs=DockerObserver(Path('/c11-paper/docker-observer.sqlite'),publication_binding,
                           wall,mono,production=True)
        try:
            feed=None
            bridge=CaptureBridge(pub,obs,publication_binding,lambda:feed.classification)
            nonce=secrets.token_hex(16)
            bridge.send(dict(op='C11_PAPER_BRIDGE_V1',nonce=nonce))
            need(bridge.receive(2)==dict(nonce=nonce,binding=publication_binding),'C11_DOCKER_BRIDGE_BOOT')
            feed=EntryFeed(bridge,generation_floor)
            yield feed,ObserverClient(bridge,publication_binding,docked_current=True)
        finally:
            obs.close()
    finally:
        pub.close()
