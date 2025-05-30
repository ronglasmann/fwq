import base64
import json
import os
from types import SimpleNamespace
from typing import Optional

import requests
from gwerks import emitter, fnow_w_ms
from gwerks.docker import DockerBase

from fwq.constants import JOB_STATE_ALL
# from fwq.docker import DockerBase
import fwq.key
from fwq.job import JobSpec

from tenacity import Retrying, stop_after_delay, wait_fixed

_clients = {}


class EtcdClient:
    def __init__(self, broker_id):
        self._broker_id = broker_id
        broker_id_nfo = fwq.key.broker_id_parser(broker_id)
        self._host = broker_id_nfo.host
        self._port = broker_id_nfo.etcd_port
        self._health_url = f'http://{self._host}:{self._port}/readyz'
        self._base_url = f'http://{self._host}:{self._port}/v3/kv'
        # print(f"EtcdClient._base_url: {self._base_url}")
        self._ready()

    def _b64(self, reg_str: str) -> str:
        # json_string = json.dumps(data)
        reg_bytes = reg_str.encode('utf-8')
        base64_bytes = base64.b64encode(reg_bytes)
        base64_string = base64_bytes.decode('utf-8')
        return base64_string

    def _reg(self, b64_str: str) -> str:
        base64_bytes = b64_str.encode("UTF-8")
        reg_bytes = base64.b64decode(base64_bytes)
        reg_str = reg_bytes.decode('utf-8')
        # data = json.loads(json.loads(json_string))
        return reg_str

    def _make_detail_key(self, app, job_id):
        return f"/{self._broker_id}/{app}/{job_id}"

    def _make_list_key(self, app):
        return f"/{self._broker_id}/{app}/jobs"

    def _set_update_time(self, app, job_id, new_state=None):
        timestamp = fnow_w_ms()

        detail_key = self._make_detail_key(app, job_id)
        self._put(f"{detail_key}/updated", timestamp)

        list_key = self._make_list_key(app)
        the_state = self._get(f"{detail_key}/state")
        if new_state:
            if the_state != new_state:
                self._delete(f"{list_key}/{the_state}/{job_id}")
                self._put(f"{list_key}/{new_state}/{job_id}", timestamp)
                the_state = new_state
            self._put(f"{detail_key}/state", new_state)
        self._put(f"{list_key}/{the_state}/{job_id}", timestamp)
        return detail_key

    def job_is_canceled(self, app, job_id):
        detail_key = self._make_detail_key(app, job_id)
        return self._get(f"{detail_key}/canceled") == "yes"

    def job_cancel(self, app, job_id):
        detail_key = self._set_update_time(app, job_id)
        self._put(f"{detail_key}/canceled", "yes")

    def update_job_result(self, app, job_id, result: str):
        detail_key = self._set_update_time(app, job_id)
        self._put(f"{detail_key}/result", result)

    def update_job_state(self, app, job_id, new_state):
        self._set_update_time(app, job_id, new_state=new_state)

    def add_job_activity(self, app, job_id, timestamp, message):
        detail_key = self._set_update_time(app, job_id)
        self._put(f"{detail_key}/activity/{timestamp}", message)

    def save_job_spec(self, app, job_spec: JobSpec):
        job_id = job_spec.get_job_id()
        new_state = job_spec.get_state()
        detail_key = self._set_update_time(app, job_id, new_state=new_state)

        self._put(f"{detail_key}/job_id", str(job_spec.get_job_id()))
        self._put(f"{detail_key}/type", job_spec.get_job_type())
        self._put(f"{detail_key}/name", job_spec.get_name())
        self._put(f"{detail_key}/data", json.dumps(job_spec.get_data()))
        self._put(f"{detail_key}/result", job_spec.get_result())

        # self._put(f"{detail_key}/state", job_spec.get_state())
        # self._put(f"{detail_key}/updated", fnow_w_ms())

        for activity in job_spec.get_activity():
            self._put(f"{detail_key}/activity/{activity['timestamp']}", activity['message'])

        return detail_key

    def delete_job_spec(self, app, job_id):
        # detail_key = job_key
        detail_key = self._make_detail_key(app, job_id)
        list_key = self._make_list_key(app)
        # job_nfo = fwq.key.parser(job_key)
        # list_key = f"/{job_nfo.broker_id}/{job_nfo.app}/jobs"
        # list_key = f"{job_key}/jobs"
        old_state = self._get(f"{detail_key}/state")
        self._delete(f"{list_key}/{old_state}/{job_id}")
        self._delete(f"{detail_key}/type")
        self._delete(f"{detail_key}/name")
        self._delete(f"{detail_key}/state")
        self._delete(f"{detail_key}/data")
        self._delete(f"{detail_key}/result")
        self._delete(f"{detail_key}/updated")
        self._delete_range(f"{detail_key}/activity/", f"{detail_key}/activity0")

    def get_job_spec(self, app, job_id) -> JobSpec:
        detail_key = self._make_detail_key(app, job_id)
        # detail_key = f"{job_key}"
        job_id = int(self._get(f"{detail_key}/job_id"))
        job_type = self._get(f"{detail_key}/type")
        name = self._get(f"{detail_key}/name")
        state = self._get(f"{detail_key}/state")
        data_str = self._get(f"{detail_key}/data")
        data = None
        if data_str:
            data = json.loads(data_str)
        updated = self._get(f"{detail_key}/updated")
        job_spec = JobSpec(job_id, job_type, name, state, data, updated)

        result = self._get(f"{detail_key}/result")
        job_spec.set_result(result)

        return job_spec

    def list_job_briefs(self, app, states=None) -> list[dict]:
        job_ids = []

        list_key = self._make_list_key(app)

        if states is None:
            states = [JOB_STATE_ALL]

        for s in states:
            key_start = f"{list_key}/{s}/"
            key_end = f"{list_key}/{s}0"
            if s == JOB_STATE_ALL:
                key_start = f"{list_key}/"
                key_end = f"{list_key}0"
            job_kvs = self._get_range(key_start, key_end)
            # print(f"job_kvs: {job_kvs}")
            for kv in job_kvs:
                job_key_nfo = fwq.key.parser(kv.key, key_type="list")
                job_ids.append({"job_key": kv.key, "updated": kv.value, "state": job_key_nfo.state, "job_id": job_key_nfo.job_id})

        return job_ids

    def _ready(self):
        response = requests.get(f'{self._health_url}')
        # print(f"_ready: {response}")
        # print(f"_ready: {response.text}")
        # print(f"_ready: {response.ok}")
        if not response.ok:
            raise Exception(f"etcd not ready: {response.text}")

    def _put(self, key, value: str):
        # print(f"_put.key: {key}")
        # print(f"_put.value: {value}")
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps({"key": self._b64(key), "value": self._b64(value)})
        response = requests.post(f'{self._base_url}/put', headers=headers, data=payload)
        # print(f"response.text: {response.text}")
        if not response.ok:
            raise Exception(f"etcd put ERROR: {response.text}")

    def _delete(self, key):
        # print(f"_delete key: {key}")
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps({"key": self._b64(key)})
        response = requests.post(f'{self._base_url}/deleterange', headers=headers, data=payload)
        # print(f"response.text: {response.text}")
        if not response.ok:
            raise Exception(f"etcd delete ERROR: {response.text}")

    def _delete_range(self, key_start, key_end):
        # print(f"_delete_range key_start: {key_start}")
        # print(f"_delete_range key_end: {key_end}")
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps({'key': self._b64(key_start), 'range_end': self._b64(key_end)})
        response = requests.post(f'{self._base_url}/deleterange', headers=headers, data=payload)
        # print(f"response.text: {response.text}")
        if not response.ok:
            raise Exception(f"etcd delete_range ERROR: {response.text}")

    def _get(self, key) -> Optional[str]:
        # print(f"_get.key: {key}")
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps({'key': self._b64(key)})
        response = requests.post(f'{self._base_url}/range', headers=headers, data=payload)
        # print(f"response.text: {response.text}")
        if not response.ok:
            raise Exception(f"etcd get ERROR: {response.text}")
        json_obj = response.json()
        # print(f"json: {json_obj}")
        if 'kvs' not in json_obj:
            return None
        if len(json_obj['kvs']) == 0:
            return None
        if 'value' not in json_obj['kvs'][0]:
            return None
        value = self._reg(json_obj['kvs'][0]['value'])
        # print(f"job_spec: {job_spec}, {type(job_spec)}")
        return value

    def _get_range(self, key_start, key_end) -> list[SimpleNamespace]:
        # print(f"_get_range.key_start: {key_start}")
        # print(f"_get_range.key_end: {key_end}")
        kv_list = []
        headers = {'Content-Type': 'application/json'}
        payload = json.dumps({'key': self._b64(key_start), 'range_end': self._b64(key_end), 'limit': "0"})
        response = requests.post(f'{self._base_url}/range', headers=headers, data=payload)
        # print(f"response.text: {response.text}")
        if not response.ok:
            raise Exception(f"etcd get ERROR: {response.text}")
        json_obj = response.json()
        # print(f"json: {json_obj}")
        if 'kvs' in json_obj:
            if len(json_obj['kvs']) > 0:
                for kv in json_obj['kvs']:
                    if 'key' in kv and 'value' in kv:
                        key = self._reg(kv['key'])
                        value = self._reg(kv['value'])
                        kv_list.append(SimpleNamespace(key=key, value=value))
        # print(f"key_list: {key_list}")
        return kv_list


@emitter()
class Etcd(DockerBase):
    def __init__(self, config):

        # container_name, network: Network, client_port=, peering_port=, data_volume_host=None
        config['image_name'] = "bitnami/etcd:latest"
        if "port" not in config:
            config["port"] = "2379"
        super().__init__(config)

        self._data_volume_host = None
        if "data_volume_host" in config:
            self._data_volume_host = config["data_volume_host"]
        # self._data_volume_host = os.path.abspath(self._data_volume_host)
        self._data_volume_host = f"{os.path.abspath(self._data_volume_host).replace("\\", "/")}"

        self._data_volume = "/bitnami/etcd/data"
        if "data_volume" in config:
            self._data_volume = config["data_volume"]
        # self._data_volume = os.path.abspath(self._data_volume)

        self._peering_port = "2380"
        if "peering_port" in config:
            self._peering_port = config["peering_port"]

        self._published_ports.append(self._peering_port)
        if self._data_volume_host:
            self._volume_mappings.append([self._data_volume_host, self._data_volume])

    def start(self):
        # self.get_docker_network().create()
        env = ""
        env += f"--env ALLOW_NONE_AUTHENTICATION=yes "
        env += f"--env ETCD_ADVERTISE_CLIENT_URLS=http://{self.get_docker_container_name()}:2379 "
        self.docker_run(env_vars=env)

    def stop(self):
        self.docker_stop()


def get_etcd_client(broker_id, timeout_secs=10, retry_secs=1) -> EtcdClient:
    # print(f"broker_id: {broker_id}")
    client = _clients.get(broker_id, None)
    if not client:
        for attempt in Retrying(reraise=True, stop=stop_after_delay(timeout_secs), wait=wait_fixed(retry_secs)):
            with attempt:
                print(f"Attempting Etcd connection: {broker_id} ...")
                client = EtcdClient(broker_id)
        _clients[broker_id] = client
    return client

