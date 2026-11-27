"""Owner-only exact accepted planner items; pending does not confer coverage."""
from threading import get_ident

class PendingRoster:
    def __init__(self):
        self.owner=get_ident()
        self.marker=None
        self.items={}
        self.fenced=0
        self.expired=0

    def _owned(self):
        if get_ident()!=self.owner:raise RuntimeError('ORIGINAL_PENDING_OWNER')

    def clear(self):
        self._owned()
        self.fenced+=len(self.items)
        self.items.clear()
        self.marker=None

    def plan(self, marker, offered, now, verified_shape, accepted_symbols):
        self._owned()
        if marker!=self.marker:
            self.clear()
            self.marker=marker
        present={}
        lanes=set()
        for task,scope,identity in offered:
            if not verified_shape(scope) or (scope.generation,scope.end)!=marker:
                continue
            if (scope.symbol not in accepted_symbols or identity in present
                    or task.tf not in ('15m','1m','5m') or (scope.symbol,task.tf) in lanes):
                # Reject unrelated/duplicate lane without slicing current roster.
                self.clear()
                return ()
            lanes.add((scope.symbol,task.tf))
            if not now<scope.expires:
                self.expired+=int(identity in self.items)
                continue
            prior=self.items.get(identity)
            if prior and (prior[0] is not task or prior[1]!=scope):
                # The original planner freezes the ticket+task; refuse equivocation.
                self.clear()
                return ()
            present[identity]=(task,scope,identity)
        self.fenced+=len(set(self.items)-set(present))
        # Stable earlier pending positions, then new exact proof in rank order.
        ordered={k:present[k] for k in self.items if k in present}
        ordered.update(present)
        self.items=ordered
        return tuple(ordered.values())

    def retire(self,identity):
        self._owned()
        self.items.pop(identity,None)
