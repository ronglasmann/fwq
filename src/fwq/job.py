
'''
{broker_id}/{app}/jobs/{state}/{job_id}: {timestamp}

{broker_id}/{app}/{job_id}/type: {type}
{broker_id}/{app}/{job_id}/name: {name}
{broker_id}/{app}/{job_id}/data: {data}
{broker_id}/{app}/{job_id}/state: {state}
{broker_id}/{app}/{job_id}/activity/{timestamp}: {message}
'''
from typing import Optional

from gwerks import fnow_w_ms


class JobSpec:
    def __init__(self, job_id: int, job_type:  Optional[str], name: Optional[str], state: str, data: Optional[dict], updated: str):
        self._job_id = job_id
        self._job_type = job_type
        self._name = name
        self._state = state
        self._data = data
        self._updated = updated
        self._qed = False
        self._activity = []
        self._result = ""

    def __repr__(self):
        return (f"JobSpec("
                f"job_id={self._job_id}', "
                f"job_type={self._job_type}, "
                f"name={self._name}, "
                f"state={self._state}, "
                f"data={self._data}, "
                f"updated={self._updated}, "
                f"qed={self._qed}, "
                f"activity={self._activity}, "
                f"result={self._result}"
                f")")

    @staticmethod
    def from_dict(job_spec_dict: dict):
        job_id = int(job_spec_dict["job_id"])
        job_type = job_spec_dict["job_type"]
        name = job_spec_dict["name"]
        state = job_spec_dict["state"]
        data = job_spec_dict["data"]
        updated = job_spec_dict["updated"]
        job_spec = JobSpec(job_id, job_type, name, state, data, updated)
        if "activity" in job_spec_dict:
            activity = job_spec_dict["activity"]
            job_spec.set_activity(activity)
        if "result" in job_spec_dict:
            result = job_spec_dict["result"]
            job_spec.set_result(result)
        return job_spec

    def to_dict(self) -> dict:
        return {
            "job_id": self._job_id,
            "job_type": self._job_type,
            "name": self._name,
            "state": self._state,
            "data": self._data,
            "updated": self._updated,
            "activity": self._activity,
            "result": self._result,
            "qed": self._qed
        }

    def get_state(self) -> str:
        return self._state

    def set_state(self, state: str) -> None:
        self._state = state

    def get_updated(self) -> str:
        return self._updated

    def set_updated_now(self):
        self._updated = fnow_w_ms()

    def is_qed(self) -> bool:
        return self._qed

    def set_qed(self, qed: bool):
        self._qed = qed

    def set_result(self, result: str):
        self._result = result

    def get_result(self) -> str:
        return self._result

    def set_activity(self, activity: list):
        self._activity = activity

    def add_activity(self, timestamp: str, message: str):
        activity = {"timestamp": timestamp, "message": message}
        self._activity.append(activity)

    def get_activity(self) -> list:
        return self._activity

    def get_job_id(self) -> int:
        return self._job_id

    def get_job_type(self) -> str:
        return self._job_type

    def get_name(self) -> str:
        return self._name

    def get_state(self) -> str:
        return self._state

    def get_data(self) -> dict:
        return self._data

