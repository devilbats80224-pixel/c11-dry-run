"""Build-only release pin, no writable configuration or environment activation."""
# A later release cannot flip a bool. It must supply an actual trusted compiled
# factory for the existing scheduler and original runtime owner, then reseal.
OWNED_SCHEDULER_FACTORY = None
OWNED_SCHEDULER_EXPECTED_SEAL = None
