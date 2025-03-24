def disable_resource_tracker():
    from multiprocessing import resource_tracker

    class DummyTracker(resource_tracker.ResourceTracker):
        def register(self, name, rtype):
            pass

        def unregister(self, name, rtype):
            pass

        def ensure_running(self):
            pass

    resource_tracker._resource_tracker = DummyTracker()
    resource_tracker.register = resource_tracker._resource_tracker.register
    resource_tracker.ensure_running = resource_tracker._resource_tracker.ensure_running
    resource_tracker.unregister = resource_tracker._resource_tracker.unregister
    resource_tracker.getfd = resource_tracker._resource_tracker.getfd


class InternalError(Exception):
    ...
