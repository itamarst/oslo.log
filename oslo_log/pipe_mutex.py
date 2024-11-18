# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import errno
import fcntl

import eventlet
import eventlet.asyncio
import eventlet.debug
import eventlet.greenthread
import eventlet.hubs
import eventlet.hubs.asyncio
import eventlet.patcher

# We want the blocking APIs, because we set file descriptors to non-blocking.
os = eventlet.patcher.original("os")


class _BaseMutex:
    """Shared code for different mutex implementations."""

    def __init__(self):
        self.owner = None
        self.recursion_depth = 0

    def acquire(self, blocking=True):
        """Acquire the mutex.

        If called with blocking=False, returns True if the mutex was
        acquired and False if it wasn't. Otherwise, blocks until the mutex
        is acquired and returns True.
        This lock is recursive; the same greenthread may acquire it as many
        times as it wants to, though it must then release it that many times
        too.
        """
        current_greenthread_id = id(eventlet.greenthread.getcurrent())
        if self.owner == current_greenthread_id:
            self.recursion_depth += 1
            return True

        return self._acquire(blocking, current_greenthread_id)

    def release(self):
        """Release the mutex."""
        current_greenthread_id = id(eventlet.greenthread.getcurrent())
        if self.owner != current_greenthread_id:
            raise RuntimeError("cannot release un-acquired lock")

        if self.recursion_depth > 0:
            self.recursion_depth -= 1
            return

        self.owner = None
        self._release()

    def close(self):
        """Close the mutex.

        This releases its file descriptors.
        You can't use a mutex after it's been closed.
        """
        self.owner = None
        self.recursion_depth = 0


class _ReallyPipeMutex(_BaseMutex):
    """Mutex using a pipe.

    Works across both greenlets and real threads, even at the same time.

    Class code copied from Swift's swift/common/utils.py
    Related eventlet bug: https://github.com/eventlet/eventlet/issues/432
    """
    def __init__(self):
        _BaseMutex.__init__(self)

        self.rfd, self.wfd = os.pipe()

        # You can't create a pipe in non-blocking mode; you must set it
        # later.
        rflags = fcntl.fcntl(self.rfd, fcntl.F_GETFL)
        fcntl.fcntl(self.rfd, fcntl.F_SETFL, rflags | os.O_NONBLOCK)
        os.write(self.wfd, b'-')  # start unlocked

        # Usually, it's an error to have multiple greenthreads all waiting
        # to read the same file descriptor. It's often a sign of inadequate
        # concurrency control; for example, if you have two greenthreads
        # trying to use the same memcache connection, they'll end up writing
        # interleaved garbage to the socket or stealing part of each others'
        # responses.
        #
        # In this case, we have multiple greenthreads waiting on the same
        # file descriptor by design. This lets greenthreads in real thread A
        # wait with greenthreads in real thread B for the same mutex.
        # Therefore, we must turn off eventlet's multiple-reader detection.
        #
        # It would be better to turn off multiple-reader detection for only
        # our calls to trampoline(), but eventlet does not support that.
        eventlet.debug.hub_prevent_multiple_readers(False)

    def _acquire(self, blocking, current_greenthread_id):
        while True:
            try:
                # If there is a byte available, this will read it and remove
                # it from the pipe. If not, this will raise OSError with
                # errno=EAGAIN.
                os.read(self.rfd, 1)
                self.owner = current_greenthread_id
                return True
            except OSError as err:
                if err.errno != errno.EAGAIN:
                    raise

                if not blocking:
                    return False

                # Tell eventlet to suspend the current greenthread until
                # self.rfd becomes readable. This will happen when someone
                # else writes to self.wfd.
                eventlet.hubs.trampoline(self.rfd, read=True)

    def _release(self):
        os.write(self.wfd, b'X')

    def close(self):
        """Close the mutex.

        This releases its file descriptors.
        You can't use a mutex after it's been closed.
        """
        if self.wfd is not None:
            os.close(self.wfd)
            self.wfd = None
        if self.rfd is not None:
            os.close(self.rfd)
            self.rfd = None
        _BaseMutex.close(self)

    def __del__(self):
        # We need this so we don't leak file descriptors. Otherwise, if you
        # call get_logger() and don't explicitly dispose of it by calling
        # logger.logger.handlers[0].lock.close() [1], the pipe file
        # descriptors are leaked.
        #
        # This only really comes up in tests. Service processes tend to call
        # get_logger() once and then hang on to it until they exit, but the
        # test suite calls get_logger() a lot.
        #
        # [1] and that's a completely ridiculous thing to expect callers to
        # do, so nobody does it and that's okay.
        self.close()


class _AsyncioMutex(_BaseMutex):
    """Alternative implementation of mutex for eventlet asyncio hub.

    When using the eventlet asyncio hub, multiple file descriptors as readers
    aren't supported.  So instead we use an asyncio Lock.
    """
    def __init__(self):
        _BaseMutex.__init__(self)
        self._asyncio_lock = asyncio.Lock()
        # The asyncio loop in the main thread:
        self._loop = _HUB.loop

    async def _underlying_acquire(self, timeout, current_greenthread_id):
        try:
            await asyncio.wait_for(
                self._asyncio_lock.acquire(), timeout=timeout
            )
        except TimeoutError:
            return False
        else:
            self.owner = current_greenthread_id
            return True

    def _run_in_asyncio_thread(self, func):
        # If we're in the asyncio thread, we can just call this directly:
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None
        if current_loop == self._loop:
            return func()

        return asyncio.run_coroutine_threadsafe(
            func(), self._loop
        ).wait()

    def _acquire(self, blocking, current_greenthread_id):
        if blocking:
            timeout = None
        else:
            timeout = 0.000001

        return self._run_in_asyncio_thread(
            lambda: eventlet.asyncio.spawn_for_awaitable(
                self._underlying_acquire(timeout, current_greenthread_id)
            ).wait()
        )

    def _release(self):
        return self._run_in_asyncio_thread(lambda: self._asyncio_lock.release())

    def close(self):
        """Close the mutex."""
        del self._asyncio_lock
        _BaseMutex.close(self)


_HUB = eventlet.hubs.get_hub()
if isinstance(_HUB, eventlet.hubs.asyncio.Hub):
    PipeMutex = _AsyncioMutex
else:
    PipeMutex = _ReallyPipeMutex


def pipe_createLock(self):
    """Replacement for logging.Handler.createLock method."""
    self.lock = PipeMutex()
