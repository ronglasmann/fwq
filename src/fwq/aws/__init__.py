from time import time
from datetime import timedelta

import boto3

from gwerks import now
from fwq.logs import Sender


class CloudWatchLogsSender(Sender):
    def __init__(self, log_group, log_stream, batch_timespan_in_minutes=5, max_batch_items=100, max_batch_size_bytes=1048576):

        if max_batch_size_bytes > 1048576:
            raise Exception(f"max_batch_size_bytes too big, must be 1,048,576 or less")

        if max_batch_items > 10000:
            raise Exception(f"max_batch_size too big, must be 10,000 or less")

        fouteen_days_in_minutes = 60 * 24 * 14
        if batch_timespan_in_minutes > fouteen_days_in_minutes:
            raise Exception(f"batch_timespan_in_minutes too big, must be less than {fouteen_days_in_minutes}")

        self._cw_logs = boto3.client('logs')
        self._log_group_name = log_group
        self._log_stream_name = log_stream
        self._batch_timespan_in_minutes = batch_timespan_in_minutes
        self._max_batch_items = max_batch_items
        self._max_batch_size_bytes = max_batch_size_bytes

        self._make_new_batch()

    def __repr__(self):
        return f"<CloudWatchLogsSender log_group:{self._log_group_name} log_stream:{self._log_stream_name}>"

    def send(self, msg):

        kb256 = 256 * 1024
        msg_len = len(msg)

        # truncate messages that are too long
        if msg_len > kb256:
            msg = msg[:kb256]

        # flush if batch is too big
        if self._batch_size_bytes + msg_len + 26 > self._max_batch_size_bytes:
            self.flush()

        # batch am event
        log_event = {
            'timestamp': time(),
            'message': msg
        }
        self._log_event_batch.append(log_event)
        self._batch_size_bytes += msg_len + 26

        # flush if batch is full
        if len(self._log_event_batch) >= self._max_batch_items:
            self.flush()

        # flush if batch is aging out
        if now() > self._batch_start_time + timedelta(minutes=self._batch_timespan_in_minutes):
            self.flush()

    def flush(self):
        resp = self._cw_logs.put_log_events(
            logGroupName=self._log_group_name,
            logStreamName=self._log_stream_name,
            logEvents=self._log_event_batch,
            # entity={
            #     'keyAttributes': {
            #         'string': 'string'
            #     },
            #     'attributes': {
            #         'string': 'string'
            #     }
            # }
        )
        # print(f"{yaml.dump(resp)}")
        self._make_new_batch()

        results = ""
        if "rejectedLogEventsInfo" in resp:
            results += f"Log events were rejected: {resp['rejectedLogEventsInfo']}. "
        if "rejectedEntityInfo" in resp:
            results += f"Entities were rejected: {resp['rejectedEntityInfo']}. "
        if results != "":
            raise Exception(results)

    def _make_new_batch(self):
        self._log_event_batch = []
        self._batch_start_time = now()
        self._batch_size_bytes = 0