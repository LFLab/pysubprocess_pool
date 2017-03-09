import os
import sys
import queue
import atexit
import weakref
import threading
from subprocess import Popen, PIPE
from concurrent.futures import _base

__all__ = ("SubprocessPoolExecutor", "DEFAULT_ENCODING")

DEFAULT_ENCODING = sys.getdefaultencoding()
_mgmt_thread = weakref.WeakKeyDictionary()
_shutdown = False


@atexit.register
def _python_exit():
    global _shutdown
    _shutdown = True
    items = list(_mgmt_thread.items())
    for t, q in items:
        q.put(None)
        t.join()


def _management_worker(executor_reference, work_queue):
    pending_proc = set()
    try:
        while True:
            pending_proc -= {w for w in pending_proc if w.done()}
            executor = executor_reference()
            max_workers = executor._max_workers \
                if executor else work_queue.qsize() + 1

            if len(pending_proc) == 0:
                work_item = work_queue.get()
            elif len(pending_proc) < max_workers:
                try:
                    work_item = work_queue.get_nowait()
                except queue.Empty:
                    work_item = None
            else:
                continue

            if work_item and not work_item.done():
                pending_proc.add(work_item)
            if _shutdown or executor is None or executor._shutdown:
                return
            del work_item, executor
    except BaseException:
        _base.LOGGER.critical("Exception in worker", exc_info=True)


class _WorkItem:
    def __init__(self, future, cmd, kwargs):
        self.future = future
        self._proc = None
        self.cmd = cmd
        self.kwargs = kwargs.copy()
        self.kwargs['stdout'] = PIPE
        self.kwargs['stderr'] = PIPE
        self.kwargs['shell'] = kwargs.get("shell", True)
        self.kwargs['encoding'] = self.kwargs.get('encoding', DEFAULT_ENCODING)

    def run(self):
        try:
            if not self._proc:
                self._proc = Popen(self.cmd, **self.kwargs)
        except Exception as e:
            self.future.set_exception(e)

    def poll(self):
        if self._proc.poll() is not None:
            ok, err = self._proc.stdout.read(), self._proc.stderr.read()
            if err:
                self.future.set_exception((self._proc.returncode, err))
            else:
                self.future.set_result(ok)

        return self.future.done()

    def wait(self, timeout=None):
        code = self._proc.wait()
        self.poll()
        return code

    def done(self):
        if not self._proc:
            self.run()
        return self.poll()


class SubprocessPoolExecutor(_base.Executor):
    def __init__(self, max_workers=None):
        """Initializes a new SubprocessPoolExecutor instance.

        Args:
            max_workers: The maximum number of subprocesses that can be used to
                execute the given calls.
        """
        if max_workers is None:
            max_workers = (os.cpu_count() or 1) * 5
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        self._max_workers = max_workers
        self._work_queue = queue.Queue()
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        self._management_thread = None

    def submit(self, cmd, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown.")

            f = _base.Future()
            w = _WorkItem(f, cmd.format(*args), kwargs)

            self._work_queue.put(w)
            self._start_management_thread()
            return f

    def _start_management_thread(self):
        def weakref_cb(_, q=self._work_queue):
            q.put(None)
        if self._management_thread is None:
            self._management_thread = threading.Thread(
                    target=_management_worker,
                    args=(weakref.ref(self, weakref_cb), self._work_queue))
            self._management_thread.daemon = True
            self._management_thread.start()
            _mgmt_thread[self._management_thread] = self._work_queue

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            self._work_queue.put(None)
        if wait and self._management_thread:
            self._management_thread.join()
    shutdown.__doc__ = _base.Executor.shutdown.__doc__
