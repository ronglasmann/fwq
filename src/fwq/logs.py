"""'Context manager class to capture job output'"""

import inspect
import sys
import traceback
from typing import Protocol


class Sender(Protocol):
    def send(self, msg):
        ...

    def flush(self):
        ...


class JobLogger:
    def __init__(self, broker_id, worker_id):
        self._broker_id = broker_id
        self._worker_id = worker_id
        self._job_id = None
        self._stdout = sys.stdout
        self._streams: list[Sender] = []

    def register_logstream(self, logstream: Sender):
        self._streams.append(logstream)

    def change_jobs(self, new_job_id):
        self._job_id = new_job_id

    def __enter__(self):
        sys.stdout = self

    def write(self, msg):
        try:
            msg = msg.strip()
            if len(msg) > 0:
                msg_source = inspect.stack()[1].function
                msg = self._format_msg(f"[{msg_source}] {msg}")
                self._stdout.write(msg)
                for ls in self._streams:
                    try:
                        ls.send(msg)
                    except Exception as e:
                        err_msg = self._format_msg(f"[{msg_source}] {ls} ERROR: {e}")
                        self._stdout.write(err_msg)

                # self.flush()
        except Exception as e:
            self._stdout.write(self._format_msg(f"ERROR: {e}"))

    def _format_msg(self, msg):
        return f"JOB [{self._broker_id}]-[{self._worker_id}]-[{self._job_id}]: {msg}\n"

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            self.write(traceback.format_exc())
        sys.stdout = self._stdout

    def flush(self):
        for ls in self._streams:
            try:
                ls.flush()
            except Exception as e:
                err_msg = self._format_msg(f"{ls} ERROR: {e}")
                self._stdout.write(err_msg)
        self._stdout.flush()
