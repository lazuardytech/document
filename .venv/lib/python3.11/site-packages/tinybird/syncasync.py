# Copyright (c) Django Software Foundation and individual contributors.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#     1. Redistributions of source code must retain the above copyright notice,
#        this list of conditions and the following disclaimer.
#
#     2. Redistributions in binary form must reproduce the above copyright
#        notice, this list of conditions and the following disclaimer in the
#        documentation and/or other materials provided with the distribution.
#
#     3. Neither the name of Django nor the names of its contributors may be used
#        to endorse or promote products derived from this software without
#        specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import asyncio.coroutines
import functools
import inspect
import os
import queue
import random
import string
import sys
import threading
import warnings
import weakref
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional

if sys.version_info >= (3, 7):
    import contextvars
else:
    contextvars = None


class Local:
    """
    A drop-in replacement for threading.locals that also works with asyncio
    Tasks (via the current_task asyncio method), and passes locals through
    sync_to_async and async_to_sync.
    Specifically:
     - Locals work per-coroutine on any thread not spawned using asgiref
     - Locals work per-thread on any thread not spawned using asgiref
     - Locals are shared with the parent coroutine when using sync_to_async
     - Locals are shared with the parent thread when using async_to_sync
       (and if that thread was launched using sync_to_async, with its parent
       coroutine as well, with this working for indefinite levels of nesting)
    Set thread_critical to True to not allow locals to pass from an async Task
    to a thread it spawns. This is needed for code that truly needs
    thread-safety, as opposed to things used for helpful context (e.g. sqlite
    does not like being called from a different thread to the one it is from).
    Thread-critical code will still be differentiated per-Task within a thread
    as it is expected it does not like concurrent access.
    This doesn't use contextvars as it needs to support 3.6. Once it can support
    3.7 only, we can then reimplement the storage more nicely.
    """

    CLEANUP_INTERVAL = 60  # seconds

    def __init__(self, thread_critical: bool = False) -> None:
        self._thread_critical = thread_critical
        self._thread_lock = threading.RLock()
        self._context_refs: "weakref.WeakSet[object]" = weakref.WeakSet()
        # Random suffixes stop accidental reuse between different Locals,
        # though we try to force deletion as well.
        self._attr_name = "_asgiref_local_impl_{}_{}".format(
            id(self),
            "".join(random.choice(string.ascii_letters) for i in range(8)),
        )

    def _get_context_id(self):
        """
        Get the ID we should use for looking up variables
        """
        # Prevent a circular reference
        # from .sync import AsyncToSync, SyncToAsync

        # First, pull the current task if we can
        context_id = SyncToAsync.get_current_task()
        context_is_async = True
        # OK, let's try for a thread ID
        if context_id is None:
            context_id = threading.current_thread()
            context_is_async = False
        # If we're thread-critical, we stop here, as we can't share contexts.
        if self._thread_critical:
            return context_id
        # Now, take those and see if we can resolve them through the launch maps
        for i in range(sys.getrecursionlimit()):  # noqa: B007
            try:
                if context_is_async:
                    # Tasks have a source thread in AsyncToSync
                    context_id = AsyncToSync.launch_map[context_id]
                    context_is_async = False
                else:
                    # Threads have a source task in SyncToAsync
                    context_id = SyncToAsync.launch_map[context_id]
                    context_is_async = True
            except KeyError:
                break
        else:
            # Catch infinite loops (they happen if you are screwing around
            # with AsyncToSync implementations)
            raise RuntimeError("Infinite launch_map loops")
        return context_id

    def _get_storage(self):
        context_obj = self._get_context_id()
        if not hasattr(context_obj, self._attr_name):
            setattr(context_obj, self._attr_name, {})
            self._context_refs.add(context_obj)
        return getattr(context_obj, self._attr_name)

    def __del__(self):
        try:
            for context_obj in self._context_refs:
                try:
                    delattr(context_obj, self._attr_name)
                except AttributeError:
                    pass
        except TypeError:
            # WeakSet.__iter__ can crash when interpreter is shutting down due
            # to _IterationGuard being None.
            pass

    def __getattr__(self, key):
        with self._thread_lock:
            storage = self._get_storage()
            if key in storage:
                return storage[key]
            else:
                raise AttributeError(f"{self!r} object has no attribute {key!r}")

    def __setattr__(self, key, value):
        if key in ("_context_refs", "_thread_critical", "_thread_lock", "_attr_name"):
            return super().__setattr__(key, value)
        with self._thread_lock:
            storage = self._get_storage()
            storage[key] = value

    def __delattr__(self, key):
        with self._thread_lock:
            storage = self._get_storage()
            if key in storage:
                del storage[key]
            else:
                raise AttributeError(f"{self!r} object has no attribute {key!r}")


class _WorkItem:
    """
    Represents an item needing to be run in the executor.
    Copied from ThreadPoolExecutor (but it's private, so we're not going to rely on importing it)
    """

    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if not self.future.set_running_or_notify_cancel():
            return
        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as exc:
            self.future.set_exception(exc)
            # Break a reference cycle with the exception 'exc'
            self = None
        else:
            self.future.set_result(result)


class CurrentThreadExecutor(Executor):
    """
    An Executor that actually runs code in the thread it is instantiated in.
    Passed to other threads running async code, so they can run sync code in
    the thread they came from.
    """

    def __init__(self):
        self._work_thread = threading.current_thread()
        self._work_queue = queue.Queue()
        self._broken = False

    def run_until_future(self, future):
        """
        Runs the code in the work queue until a result is available from the future.
        Should be run from the thread the executor is initialised in.
        """
        # Check we're in the right thread
        if threading.current_thread() != self._work_thread:
            raise RuntimeError("You cannot run CurrentThreadExecutor from a different thread")
        future.add_done_callback(self._work_queue.put)
        # Keep getting and running work items until we get the future we're waiting for
        # back via the future's done callback.
        try:
            while True:
                # Get a work item and run it
                work_item = self._work_queue.get()
                if work_item is future:
                    return
                work_item.run()
                del work_item
        finally:
            self._broken = True

    def submit(self, fn, *args, **kwargs):
        # Check they're not submitting from the same thread
        if threading.current_thread() == self._work_thread:
            raise RuntimeError("You cannot submit onto CurrentThreadExecutor from its own thread")
        # Check they're not too late or the executor errored
        if self._broken:
            raise RuntimeError("CurrentThreadExecutor already quit or is broken")
        # Add to work queue
        f = Future()
        work_item = _WorkItem(f, fn, args, kwargs)
        self._work_queue.put(work_item)
        # Return the future
        return f


def _restore_context(context):
    # Check for changes in contextvars, and set them to the current
    # context for downstream consumers
    for cvar in context:
        try:
            if cvar.get() != context.get(cvar):
                cvar.set(context.get(cvar))
        except LookupError:
            cvar.set(context.get(cvar))


def _iscoroutinefunction_or_partial(func: Any) -> bool:
    # Python < 3.8 does not correctly determine partially wrapped
    # coroutine functions are coroutine functions, hence the need for
    # this to exist. Code taken from CPython.
    if sys.version_info >= (3, 8):
        return asyncio.iscoroutinefunction(func)
    else:
        while inspect.ismethod(func):
            func = func.__func__
        while isinstance(func, functools.partial):
            func = func.func

        return asyncio.iscoroutinefunction(func)


class ThreadSensitiveContext:
    """Async context manager to manage context for thread sensitive mode
    This context manager controls which thread pool executor is used when in
    thread sensitive mode. By default, a single thread pool executor is shared
    within a process.
    In Python 3.7+, the ThreadSensitiveContext() context manager may be used to
    specify a thread pool per context.
    In Python 3.6, usage of this context manager has no effect.
    This context manager is re-entrant, so only the outer-most call to
    ThreadSensitiveContext will set the context.
    """

    def __init__(self):
        self.token = None

    if contextvars:

        async def __aenter__(self):
            try:
                SyncToAsync.thread_sensitive_context.get()
            except LookupError:
                self.token = SyncToAsync.thread_sensitive_context.set(self)

            return self

        async def __aexit__(self, exc, value, tb):
            if not self.token:
                return

            executor = SyncToAsync.context_to_thread_executor.pop(self, None)
            if executor:
                executor.shutdown()
            SyncToAsync.thread_sensitive_context.reset(self.token)

    else:

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc, value, tb):
            pass


class AsyncToSync:
    """
    Utility class which turns an awaitable that only works on the thread with
    the event loop into a synchronous callable that works in a subthread.
    If the call stack contains an async loop, the code runs there.
    Otherwise, the code runs in a new loop in a new thread.
    Either way, this thread then pauses and waits to run any thread_sensitive
    code called from further down the call stack using SyncToAsync, before
    finally exiting once the async task returns.
    """

    # Maps launched Tasks to the threads that launched them (for locals impl)
    launch_map: "Dict[asyncio.Task[object], threading.Thread]" = {}

    # Keeps track of which CurrentThreadExecutor to use. This uses an asgiref
    # Local, not a threadlocal, so that tasks can work out what their parent used.
    executors = Local()

    def __init__(self, awaitable, force_new_loop=False):
        if not callable(awaitable) or not _iscoroutinefunction_or_partial(awaitable):
            # Python does not have very reliable detection of async functions
            # (lots of false negatives) so this is just a warning.
            warnings.warn("async_to_sync was passed a non-async-marked callable", stacklevel=2)
        self.awaitable = awaitable
        try:
            self.__self__ = self.awaitable.__self__
        except AttributeError:
            pass
        if force_new_loop:
            # They have asked that we always run in a new sub-loop.
            self.main_event_loop = None
        else:
            try:
                self.main_event_loop = asyncio.get_event_loop()
            except RuntimeError:
                # There's no event loop in this thread. Look for the threadlocal if
                # we're inside SyncToAsync
                main_event_loop_pid = getattr(SyncToAsync.threadlocal, "main_event_loop_pid", None)
                # We make sure the parent loop is from the same process - if
                # they've forked, this is not going to be valid any more (#194)
                if main_event_loop_pid and main_event_loop_pid == os.getpid():
                    self.main_event_loop = getattr(SyncToAsync.threadlocal, "main_event_loop", None)
                else:
                    self.main_event_loop = None

    def __call__(self, *args, **kwargs):
        # You can't call AsyncToSync from a thread with a running event loop
        try:
            event_loop = asyncio.get_event_loop()
        except RuntimeError:
            pass
        else:
            if event_loop.is_running():
                raise RuntimeError(
                    "You cannot use AsyncToSync in the same thread as an async event loop - "
                    "just await the async function directly."
                )

        if contextvars is not None:
            # Wrapping context in list so it can be reassigned from within
            # `main_wrap`.
            context = [contextvars.copy_context()]
        else:
            context = None

        # Make a future for the return information
        call_result = Future()
        # Get the source thread
        source_thread = threading.current_thread()
        # Make a CurrentThreadExecutor we'll use to idle in this thread - we
        # need one for every sync frame, even if there's one above us in the
        # same thread.
        if hasattr(self.executors, "current"):
            old_current_executor = self.executors.current
        else:
            old_current_executor = None
        current_executor = CurrentThreadExecutor()
        self.executors.current = current_executor
        # Use call_soon_threadsafe to schedule a synchronous callback on the
        # main event loop's thread if it's there, otherwise make a new loop
        # in this thread.
        try:
            awaitable = self.main_wrap(args, kwargs, call_result, source_thread, sys.exc_info(), context)

            if not (self.main_event_loop and self.main_event_loop.is_running()):
                # Make our own event loop - in a new thread - and run inside that.
                loop = asyncio.new_event_loop()
                with ThreadPoolExecutor(max_workers=1, thread_name_prefix="AsyncToSync") as loop_executor:
                    loop_future = loop_executor.submit(self._run_event_loop, loop, awaitable)
                    if current_executor:
                        # Run the CurrentThreadExecutor until the future is done
                        current_executor.run_until_future(loop_future)
                    # Wait for future and/or allow for exception propagation
                    loop_future.result()
            else:
                # Call it inside the existing loop
                self.main_event_loop.call_soon_threadsafe(self.main_event_loop.create_task, awaitable)
                if current_executor:
                    # Run the CurrentThreadExecutor until the future is done
                    current_executor.run_until_future(call_result)
        finally:
            # Clean up any executor we were running
            if hasattr(self.executors, "current"):
                del self.executors.current
            if old_current_executor:
                self.executors.current = old_current_executor
            if contextvars is not None:
                _restore_context(context[0])

        # Wait for results from the future.
        return call_result.result()

    def _run_event_loop(self, loop, coro):
        """
        Runs the given event loop (designed to be called in a thread).
        """
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(coro)
        finally:
            try:
                # mimic asyncio.run() behavior
                # cancel unexhausted async generators
                if sys.version_info >= (3, 7, 0):
                    tasks = asyncio.all_tasks(loop)
                else:
                    tasks = asyncio.Task.all_tasks(loop)
                for task in tasks:
                    task.cancel()
                loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                for task in tasks:
                    if task.cancelled():
                        continue
                    if task.exception() is not None:
                        loop.call_exception_handler(
                            {
                                "message": "unhandled exception during loop shutdown",
                                "exception": task.exception(),
                                "task": task,
                            }
                        )
                if hasattr(loop, "shutdown_asyncgens"):
                    loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()
                asyncio.set_event_loop(self.main_event_loop)

    def __get__(self, parent, objtype):
        """
        Include self for methods
        """
        func = functools.partial(self.__call__, parent)
        return functools.update_wrapper(func, self.awaitable)

    async def main_wrap(self, args, kwargs, call_result, source_thread, exc_info, context):
        """
        Wraps the awaitable with something that puts the result into the
        result/exception future.
        """
        if context is not None:
            _restore_context(context[0])

        current_task = SyncToAsync.get_current_task()
        self.launch_map[current_task] = source_thread
        try:
            # If we have an exception, run the function inside the except block
            # after raising it so exc_info is correctly populated.
            if exc_info[1]:
                try:
                    raise exc_info[1]
                except BaseException:
                    result = await self.awaitable(*args, **kwargs)
            else:
                result = await self.awaitable(*args, **kwargs)
        except BaseException as e:
            call_result.set_exception(e)
        else:
            call_result.set_result(result)
        finally:
            del self.launch_map[current_task]

            if context is not None:
                context[0] = contextvars.copy_context()


class SyncToAsync:
    """
    Utility class which turns a synchronous callable into an awaitable that
    runs in a threadpool. It also sets a threadlocal inside the thread so
    calls to AsyncToSync can escape it.
    If thread_sensitive is passed, the code will run in the same thread as any
    outer code. This is needed for underlying Python code that is not
    threadsafe (for example, code which handles SQLite database connections).
    If the outermost program is async (i.e. SyncToAsync is outermost), then
    this will be a dedicated single sub-thread that all sync code runs in,
    one after the other. If the outermost program is sync (i.e. AsyncToSync is
    outermost), this will just be the main thread. This is achieved by idling
    with a CurrentThreadExecutor while AsyncToSync is blocking its sync parent,
    rather than just blocking.
    If executor is passed in, that will be used instead of the loop's default executor.
    In order to pass in an executor, thread_sensitive must be set to False, otherwise
    a TypeError will be raised.
    """

    # If they've set ASGI_THREADS, update the default asyncio executor for now
    if "ASGI_THREADS" in os.environ:
        loop = asyncio.get_event_loop()
        loop.set_default_executor(
            ThreadPoolExecutor(max_workers=int(os.environ["ASGI_THREADS"]), thread_name_prefix="SyncToAsync_ASGI")
        )

    # Maps launched threads to the coroutines that spawned them
    launch_map: "Dict[threading.Thread, asyncio.Task[object]]" = {}

    # Storage for main event loop references
    threadlocal = threading.local()

    # Single-thread executor for thread-sensitive code
    single_thread_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="SingletonAsyncToSync")

    # Maintain a contextvar for the current execution context. Optionally used
    # for thread sensitive mode.
    if sys.version_info >= (3, 7):
        thread_sensitive_context: "contextvars.ContextVar[str]" = contextvars.ContextVar("thread_sensitive_context")
    else:
        thread_sensitive_context: None = None

    # Maintaining a weak reference to the context ensures that thread pools are
    # erased once the context goes out of scope. This terminates the thread pool.
    context_to_thread_executor: "weakref.WeakKeyDictionary[object, ThreadPoolExecutor]" = weakref.WeakKeyDictionary()

    def __init__(
        self,
        func: Callable[..., Any],
        thread_sensitive: bool = True,
        executor: Optional["ThreadPoolExecutor"] = None,
    ) -> None:
        if not callable(func) or _iscoroutinefunction_or_partial(func):
            raise TypeError("sync_to_async can only be applied to sync functions.")
        self.func = func
        functools.update_wrapper(self, func)
        self._thread_sensitive = thread_sensitive
        self._is_coroutine = asyncio.coroutines._is_coroutine  # type: ignore
        if thread_sensitive and executor is not None:
            raise TypeError("executor must not be set when thread_sensitive is True")
        self._executor = executor
        try:
            self.__self__ = func.__self__  # type: ignore
        except AttributeError:
            pass

    async def __call__(self, *args, **kwargs):
        loop = asyncio.get_event_loop()

        # Work out what thread to run the code in
        if self._thread_sensitive:
            if hasattr(AsyncToSync.executors, "current"):
                # If we have a parent sync thread above somewhere, use that
                executor = AsyncToSync.executors.current
            elif self.thread_sensitive_context and self.thread_sensitive_context.get(None):
                # If we have a way of retrieving the current context, attempt
                # to use a per-context thread pool executor
                thread_sensitive_context = self.thread_sensitive_context.get()

                if thread_sensitive_context in self.context_to_thread_executor:
                    # Re-use thread executor in current context
                    executor = self.context_to_thread_executor[thread_sensitive_context]
                else:
                    # Create new thread executor in current context
                    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ContextAsyncToSync")
                    self.context_to_thread_executor[thread_sensitive_context] = executor
            else:
                # Otherwise, we run it in a fixed single thread
                executor = self.single_thread_executor
        else:
            # Use the passed in executor, or the loop's default if it is None
            executor = self._executor

        if contextvars is not None:
            context = contextvars.copy_context()
            child = functools.partial(self.func, *args, **kwargs)
            func = context.run
            args = (child,)
            kwargs = {}
        else:
            func = self.func

        # Run the code in the right thread
        future = loop.run_in_executor(
            executor,
            functools.partial(
                self.thread_handler,
                loop,
                self.get_current_task(),
                sys.exc_info(),
                func,
                *args,
                **kwargs,
            ),
        )
        ret = await asyncio.wait_for(future, timeout=None)

        if contextvars is not None:
            _restore_context(context)

        return ret

    def __get__(self, parent, objtype):
        """
        Include self for methods
        """
        return functools.partial(self.__call__, parent)

    def thread_handler(self, loop, source_task, exc_info, func, *args, **kwargs):
        """
        Wraps the sync application with exception handling.
        """
        # Set the threadlocal for AsyncToSync
        self.threadlocal.main_event_loop = loop
        self.threadlocal.main_event_loop_pid = os.getpid()
        # Set the task mapping (used for the locals module)
        current_thread = threading.current_thread()
        if AsyncToSync.launch_map.get(source_task) == current_thread:
            # Our parent task was launched from this same thread, so don't make
            # a launch map entry - let it shortcut over us! (and stop infinite loops)
            parent_set = False
        else:
            self.launch_map[current_thread] = source_task
            parent_set = True
        # Run the function
        try:
            # If we have an exception, run the function inside the except block
            # after raising it so exc_info is correctly populated.
            if exc_info[1]:
                try:
                    raise exc_info[1]
                except BaseException:
                    return func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        finally:
            # Only delete the launch_map parent if we set it, otherwise it is
            # from someone else.
            if parent_set:
                del self.launch_map[current_thread]

    @staticmethod
    def get_current_task():
        """
        Cross-version implementation of asyncio.current_task()
        Returns None if there is no task.
        """
        try:
            if hasattr(asyncio, "current_task"):
                # Python 3.7 and up
                return asyncio.current_task()
            else:
                # Python 3.6
                return asyncio.Task.current_task()
        except RuntimeError:
            return None


# Lowercase aliases (and decorator friendliness)
async_to_sync = AsyncToSync


def sync_to_async(
    func: Callable[..., Any],
    thread_sensitive: bool = True,
    executor: Optional["ThreadPoolExecutor"] = None,
) -> SyncToAsync:
    return SyncToAsync(
        func,
        thread_sensitive=thread_sensitive,
        executor=executor,
    )
