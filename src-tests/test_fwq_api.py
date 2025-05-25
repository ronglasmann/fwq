from time import sleep

import pytest

from fwq.api import *
from fwq.constants import JOB_STATE_DONE


@pytest.fixture(scope="module")
def broker_nfo():
    print("broker_nfo")
    config_set(config_item_key="beanstalkd_binlog_host", config_item_value="data/beanstalkd")
    config_set(config_item_key="etcd_data_volume_host", config_item_value="data/etcd")
    yield sys_start()
    sys_stop()
    config_set(config_item_key="beanstalkd_binlog_host", config_item_value=None)
    config_set(config_item_key="etcd_data_volume_host", config_item_value=None)


def test_run_job(broker_nfo):
    print("test_run_job")
    broker_id = broker_nfo
    app = "test"
    job_spec = {"job_type": "fwq.jobs.test_job", "broker_id": broker_id, "app": app}
    # print(f"job_spec: {job_spec}")
    job_key = job_nq(job_spec)
    job_id = fwq.key.parser(job_key).job_id
    # print(f"job_key: {job_key}")

    job_nfo = job_get(broker_id, app, job_id)
    print(f"job_nfo before: {job_nfo}")

    assert job_nfo.get_state() == JOB_STATE_READY
    assert job_nfo.get_job_type() == job_spec['job_type']
    assert job_nfo.get_data() == job_spec['data']
    assert job_nfo.is_qed()

    job_do_all(broker_id, app, wait_secs=1, keep_waiting=False)

    job_nfo = job_get(broker_id, app, job_id)
    print(f"job_nfo after: {job_nfo}")

    assert job_nfo.get_state() == JOB_STATE_DONE
    assert not job_nfo.is_qed()

    assert len(job_list(broker_id, app, job_states=[JOB_STATE_READY])) == 0


def test_list_jobs(broker_nfo):
    print("test_list_jobs")
    broker_id = broker_nfo
    app = "test"

    num_jobs_before = len(job_list(broker_id, app))
    print(f"num_jobs_before: {num_jobs_before}")

    job_nq({"job_type": "fwq.jobs.test_job", "broker_id": broker_id, "app": app})
    job_nq({"job_type": "fwq.jobs.test_job", "broker_id": broker_id, "app": app})
    job_nq({"job_type": "fwq.jobs.test_job", "broker_id": broker_id, "app": app})

    jobs = job_list(broker_id, app)
    num_jobs_after = len(jobs)
    print(f"num_jobs_after: {num_jobs_after}")
    assert num_jobs_after - num_jobs_before == 3


def test_purge_completed_jobs(broker_nfo):
    print("test_list_jobs")
    broker_id = broker_nfo
    app = "test"

    print("sleep 7 seconds")
    sleep(7)

    print("purge completed jobs older than 5 seconds")
    job_purge_completed(broker_id, app, 5)

    jobs = job_list(broker_id, app, job_states=[JOB_STATE_DONE])
    assert len(jobs) == 0
