import os
import sys
import time
import queue
import threading
from subprocess import Popen, PIPE
from concurrent.futures import _base

DEFAULT_ENCODING = sys.getdefaultencoding()


def _management_worker(work_queue, max_workers, shutdown):
    pending_proc = set()
    while True:
        pending_proc -= {w for w in pending_proc if w.done()}
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
        elif work_item is None and shutdown.is_set():
            if len(pending_proc) > 0:
                work_queue.put(None)
                continue
            break


class _WorkItem:
    def __init__(self, future, cmd, kwargs):
        self.future = future
        self._proc = None
        self.cmd = cmd
        self.kwargs = kwargs.copy()
        self.kwargs['stdout'] = PIPE
        self.kwargs['stderr'] = PIPE
        self.kwargs['shell'] = True
        if not self.kwargs.get('encoding'):
            self.kwargs['encoding'] = DEFAULT_ENCODING

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
                self.future.set_exception(err)
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
        self._shutdown = threading.Event()
        self._shutdown_lock = threading.Lock()
        self._management_thread = None

    def submit(self, cmd, **kwargs):
        with self._shutdown_lock:
            if self._shutdown.is_set():
                raise RuntimeError("cannot schedule new futures after shutdown.")

            f = _base.Future()
            w = _WorkItem(f, cmd, kwargs)

            self._work_queue.put(w)
            self._start_management_thread()
            return f

    def map(self, cmd, *iterables, timeout=None):
        if timeout:
            end_time = timeout + time.time()
        fs = [self.submit(cmd, **{k: v for k, v in kws}) for kws in zip(*iterables)]

        def result_iterator():
            try:
                for fut in fs:
                    if timeout:
                        yield fut.result(end_time - time.time())
                    else:
                        yield fut.result()
            finally:
                for fut in fs:
                    fut.cancel()
        return result_iterator()

    def _start_management_thread(self):
        if self._management_thread is None:
            self._management_thread = threading.Thread(
                    target=_management_worker,
                    args=(self._work_queue,
                        self._max_workers,
                        self._shutdown))
            self._management_thread.daemon = True
            self._management_thread.start()

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown.set()
            self._work_queue.put(None)
        if wait and self._management_thread:
            self._management_thread.join()
