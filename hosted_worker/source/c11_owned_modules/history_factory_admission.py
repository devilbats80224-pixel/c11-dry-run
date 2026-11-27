"""Original installed factory context adapter, no external approval source."""
from history_startup_receipt import _factory_check,_factory_claim,Hold

class FactoryAdmission:
    def __init__(self,expected_manifest=None):
        self._site=self._state=self._actual=None
        self.expected_manifest=expected_manifest
    def bind_private_source(self,site,state,actual):
        if self._site is not None:raise Hold('HISTORY_STARTUP_BINDING_ALREADY_SET')
        if self.expected_manifest is not None and self.expected_manifest!=site.manifest:
            raise Hold('HISTORY_RELEASE_MANIFEST_MISMATCH')
        self._site,self._state,self._actual=site,state,actual
    def precheck(self):
        r=_factory_check(self._site,self._state)
        if self._actual() != self._site:raise Hold('HISTORY_IMAGE_OR_CANONICAL_STORE_MISMATCH')
        return r
    def claim(self):
        self.precheck()
        return _factory_claim(self._site,self._state)
