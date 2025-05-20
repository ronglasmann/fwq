from datetime import datetime, timedelta
import json
import os
from typing import Optional

from greenstalk import NotFoundError
from gwerks import fnow_w_ms, DATETIME_FORMAT_W_MS, now
from gwerks.decorators import emitter

from fwq.job import JobSpec
# from fwq.work_q import WorkQ
from fwq.worker import Worker
from fwq.constants import JOB_STATE_READY, JOB_STATE_DELAYED, CONFIG_DEFAULT, BROKERS_LIST_DEFAULT, \
    JOB_START_PERSISTENCE_DELAY_IN_SECS, JOB_STATE_DONE

import fwq.key
import fwq.etcd
import fwq.beanstalkd


# from gwerks.docker import DockerNetwork, DockerContext


CONF_CONFIG = "configuration"
CONF_BROKERS = "broker"


# --------------------------------------------------------------------------- #
# job
# --------------------------------------------------------------------------- #

def job_kick(broker_id, app, job_id=None, job_count=None):
    number_kicked = 0
    try:
        beanstalk_client = fwq.beanstalkd.get_beanstalk_client(broker_id, app)
        if job_id is not None:
            beanstalk_client.kick_job(int(job_id))
            number_kicked = 1
        elif job_count is not None:
            number_kicked = beanstalk_client.kick(int(job_count))
    except NotFoundError:
        if job_id is not None:
            print(f"KICK job {job_id} not found")
        else:
            print(f"KICK jobs not found")
    print(f"KICK {number_kicked} [broker_id: {broker_id}, app: {app}, job_id: {job_id}, job_count: {job_count}]")

def job_touch(broker_id, app, job_id):
    job_bs = _job_get(broker_id, app, job_id)[1]
    if job_bs:
        beanstalk_client = fwq.beanstalkd.get_beanstalk_client(broker_id, app)
        beanstalk_client.touch(job_bs)

    print(f"TOUCH [broker_id: {broker_id}, app: {app}, job_id: {job_id}]")


def job_is_canceled(broker_id, app, job_id):
    etcd_client = fwq.etcd.get_etcd_client(broker_id)
    return etcd_client.job_is_canceled(app, job_id)


def job_cancel(broker_id, app, job_id):
    etcd_client = fwq.etcd.get_etcd_client(broker_id)
    etcd_client.job_cancel(app, job_id)


@emitter()
def job_purge_completed(broker_id, app, older_than_secs):
    completed_jobs = job_list(broker_id, app, [JOB_STATE_DONE])
    seconds_ago = now() - timedelta(seconds=older_than_secs)
    print(f"PURGE COMPLETED more than {older_than_secs} seconds_ago, before: {seconds_ago}")
    for job in completed_jobs:
        job_datetime = datetime.strptime(job['updated'], DATETIME_FORMAT_W_MS)
        if job_datetime <= seconds_ago:
            job_purge(broker_id, app, job['job_id'])


@emitter()
def job_purge(broker_id, app, job_id):
    etcd_client = fwq.etcd.get_etcd_client(broker_id)
    etcd_client.delete_job_spec(app, job_id)

    job_bs = _job_get(broker_id, app, job_id)[1]
    if job_bs:
        beanstalk_client = fwq.beanstalkd.get_beanstalk_client(broker_id, app)
        beanstalk_client.delete(job_bs)

    print(f"PURGE [broker_id: {broker_id}, app: {app}, job_id: {job_id}]")


@emitter()
def job_do_all(broker_id, app, wait_secs, keep_waiting=True):
    worker = Worker(broker_id, app)
    worker.do_jobs(wait_for_job_secs=int(wait_secs), keep_waiting=keep_waiting)


@emitter()
def job_do_next(broker_id, app, wait_secs):
    worker = Worker(broker_id, app)
    worker.do_job(wait_for_job_secs=int(wait_secs))


@emitter()
def job_list(broker_id, app, job_states=None):
    etcd_client = fwq.etcd.get_etcd_client(broker_id)
    job_ids = etcd_client.list_job_briefs(app, states=job_states)
    return job_ids


@emitter()
def job_get(broker_id, app, job_id):
    return _job_get(broker_id, app, job_id)[0]


def _job_get(broker_id, app, job_id):
    etcd_client = fwq.etcd.get_etcd_client(broker_id)
    job_spec = etcd_client.get_job_spec(app, job_id)

    beanstalk_client = fwq.beanstalkd.get_beanstalk_client(broker_id, app)
    job_bs = None
    try:
        job_bs = beanstalk_client.peek(int(job_id))
        # print(f"job_bs: {job_bs}")
        job_spec.set_qed(True)

    except NotFoundError:
        job_spec['state'] = JOB_STATE_DONE
        job_spec.set_qed(False)
        etcd_client.update_job_state(app, job_id, JOB_STATE_DONE)

    return job_spec, job_bs


@emitter()
def job_nq(job_spec: dict):
    job_type = job_spec.get('job_type', None)
    if job_type is None:
        raise Exception('job type is *required*')
    broker_id = job_spec.get('broker_id', None)
    if broker_id is None:
        raise Exception('job broker_id is *required*')
    app = job_spec.get('app', None)
    if app is None:
        raise Exception('job app is *required*')

    config = config_get()
    beanstalkd_connection_timeout_secs = int(config.get('beanstalkd_connection_timeout_secs', '10'))
    etcd3_connection_timeout_secs = int(config.get('etcd3_connection_timeout_secs', '10'))

    job_data = job_spec.get('data', None)
    if job_data is None:
        job_data = {}
    job_spec['data'] = job_data

    job_name = job_spec.get('name', None)
    if job_name is None:
        job_name = _make_job_name(job_type, job_data)
    job_spec['name'] = job_name

    priority = int(job_spec.get('priority', '65536'))
    job_spec['priority'] = priority
    delay = int(job_spec.get('delay', '0')) + JOB_START_PERSISTENCE_DELAY_IN_SECS
    job_spec['delay'] = delay
    ttr = int(job_spec.get('ttr', '60'))
    job_spec['ttr'] = ttr

    if delay > JOB_START_PERSISTENCE_DELAY_IN_SECS:
        job_spec['state'] = JOB_STATE_DELAYED
    else:
        job_spec['state'] = JOB_STATE_READY

    job_body = json.dumps({
        "type": job_type,
        "data": job_data,
        "name": job_name
    })

    # key_prefix = f"/{broker_id}/{app}"

    beanstalk_client = fwq.beanstalkd.get_beanstalk_client(broker_id, app, timeout_secs=beanstalkd_connection_timeout_secs)
    etcd_client = fwq.etcd.get_etcd_client(broker_id, timeout_secs=etcd3_connection_timeout_secs)

    job_id = beanstalk_client.put(job_body, priority=priority, delay=delay, ttr=ttr)

    job_spec['job_id'] = job_id
    # job_key = f"{job_key}/{job_id}"
    job_spec['updated'] = fnow_w_ms()

    job_key = etcd_client.save_job_spec(app, JobSpec.from_dict(job_spec))

    print(f"NQ Job [{job_key}] - {job_name} - [priority={priority}, delay={delay}, ttr={ttr}]")
    return job_key


# --------------------------------------------------------------------------- #
# broker
# --------------------------------------------------------------------------- #

@emitter()
def broker_start():
    config = config_get()
    beanstalkd = _get_beanstalkd(config)
    etcd = _get_etcd(config)
    etcd.start()
    beanstalkd.start()
    return {
        "net_docker": {
            "name": beanstalkd.get_container_name(),
            "broker_id": f"{beanstalkd.get_container_name()}_{beanstalkd.get_port()}_{etcd.get_client_port()}"
        },
        "net_host": {
            "broker_id": f"{beanstalkd.get_host_ip()}_{beanstalkd.get_port()}_{etcd.get_client_port()}"
        }
    }


@emitter()
def broker_stop():
    config = config_get()
    beanstalkd = _get_beanstalkd(config)
    etcd = _get_etcd(config)
    beanstalkd.stop()
    etcd.stop()


def broker_add(broker_id):
    brokers = broker_list()
    brokers.append(broker_id)
    _save_broker_list(brokers)
    return broker_list()


def broker_rm(broker_id):
    brokers = broker_list()
    try:
        brokers.remove(broker_id)
    except Exception as e:
        print(f"ERROR: {e}")
    _save_broker_list(brokers)
    return broker_list()


def broker_list():
    brokers = BROKERS_LIST_DEFAULT
    file_path = _get_file_path(CONF_BROKERS)
    try:
        with open(file_path, 'r') as file:
            brokers = json.load(file)
    except Exception as e:
        print(f"ERROR: {e}")
    return brokers


# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #


def config_reset():
    config_str = json.dumps(CONFIG_DEFAULT)
    return config_set(config_str=config_str)


def config_set(config_str=None, config_item_key=None, config_item_value=None):
    if config_str:
        data = json.loads(config_str)
        # save config to a JSON file
    elif config_item_key:
        data = config_get()
        data[config_item_key] = config_item_value
    else:
        raise Exception("Either 'config_str' OR 'config_item_key' must be specified")

    file_path = _get_file_path(CONF_CONFIG)
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

    return config_get()


def config_get():
    config_dict = CONFIG_DEFAULT
    file_path = _get_file_path(CONF_CONFIG)
    try:
        with open(file_path, 'r') as file:
            config_dict = json.load(file)
    except Exception as e:
        print(f"ERROR: {e}")
    return config_dict


# --------------------------------------------------------------------------- #
# utilities
# --------------------------------------------------------------------------- #


def _make_job_name(job_type: Optional[str], job_data):
    module = job_type
    try:
        pkg, module = job_type.rsplit(".", 1)
    except Exception:
        pass
    return f"{module}-{abs(hash(json.dumps(job_data))) % (10 ** 8)}"


def _get_file_path(file_type):
    os.makedirs(os.path.expanduser("~/.fwq/"), exist_ok=True)
    return os.path.expanduser(f"~/.fwq/{file_type}.json")


def _get_etcd(config=None):
    from fwq.etcd import Etcd
    if not config:
        config = config_get()
    container_name = f"{config['broker_name']}-etcd"
    network = _get_network(config)
    client_port = config['etcd_client_port']
    peering_port = config['etcd_peering_port']
    data_volume_host = config['etcd_data_volume_host']
    return Etcd(container_name, network, client_port, peering_port, data_volume_host)


def _get_beanstalkd(config=None):
    from fwq.beanstalkd import Beanstalkd
    if not config:
        config = config_get()
    # print(f"config: {config}")
    container_name = f"{config['broker_name']}-beanstalkd"
    network = _get_network(config)
    addr = config['beanstalkd_addr']
    port = config['beanstalkd_port']
    data_volume_host = config['beanstalkd_binlog_host']
    max_message_size = config['beanstalkd_max_message_size']
    return Beanstalkd(container_name, network, addr, port, data_volume_host, max_message_size)


def _get_network(config=None):
    from fwq.docker import Network
    if not config:
        config = config_get()
    network_name = f"{config['broker_name']}-net"
    network_driver = config['docker_network_driver']
    return Network(network_name, network_driver)


def _save_broker_list(brokers):
    file_path = _get_file_path(CONF_BROKERS)
    with open(file_path, 'w') as file:
        json.dump(brokers, file, indent=4)
