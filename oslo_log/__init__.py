import os

# Only used by unit testing. Monkey patching has to happen as early as possible
# or it can break, which is why we unfortunately have to do it here and not in
# tests/__init__.py.
if os.environ.get("OSLO_LOG_TEST_EVENTLET") == "1":
    import eventlet
    eventlet.monkey_patch()
