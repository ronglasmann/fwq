import sys
import yaml
import json

from gwerks.decorators import emitter
from gwerks.cli import cli, Clo
from gwerks.docker import DockerNetwork

from fwq.broker import Broker
from fwq.constants import JOB_STATE_READY

from fwq.api import start, stop, do_next_job, do_jobs, kick_job, stats, nq, purge, peek

"""
context = DockerContext(data={
    "dockerfile_str": Broker.DOCKERFILE_BASE + "COPY gwerks.tar.gz .\nCOPY fwq.tar.gz .\nRUN pip3 install gwerks.tar.gz\nRUN pip3 install fwq.tar.gz\n",
    "files": [("../gwerks/dist/gwerks-25.2.13.tar.gz", "gwerks.tar.gz"), ("dist/fwq-0.0.0.tar.gz", "fwq.tar.gz")]
})

broker = Broker(context=context, map_to_host_port=11300)
"""


# --------------------------------------------------------------------------- #
# CLI entry point
# --------------------------------------------------------------------------- #
@emitter()
def fwq():

    _debug_traceback_limit = 1000
    sys.tracebacklimit = 0

    try:

        # command line arguments and default values
        clo = cli([
            {   # base args
                "action": Clo.REQUIRED,
                "debug": None
            },
            {   # action start, stop
                "name": Broker.DEFAULT_NAME
            },
            {   # action start
                "port_spec": Broker.DEFAULT_PORT,
                "host_volume": "./data",
                "max_message_size": "65535",
                "log_group_prefix": None,
                "net_name": None,
                "net_driver": DockerNetwork.DEFAULT_DRIVER,
                "context": None
            },
            {  # action do_next_job, kick_job, stats, nq, purge, peek
                "for": Clo.REQUIRED,
                "broker": f"{Broker.DEFAULT_NAME}:{Broker.DEFAULT_PORT}",
            },
            {  # action do_next_job
                "wait": "60"
            },
            {  # action kick_job, stats, peek
                "id": None
            },
            {  # action nq
                "job_type": Clo.REQUIRED,
                "job_data": None,
                "job_name": None,
                "priority": "65536",
                "delay_secs": "0",
                "ttr": "60",
            },
            {  # action kick_job, purge, peek
                "at_most": Clo.REQUIRED,
                "in_state": JOB_STATE_READY,
            }
        ])
        print(f"Hello!")

        debug = clo.get("debug") == "True"
        if debug:
            sys.tracebacklimit = _debug_traceback_limit

        action = clo.get("action")
        cli_action = f"cli_{action}"

        print(f"---------------------------------------------------------------------------------")

        if not action:
            raise Exception("--action | -a is required")

        globals()[cli_action](clo)

        print(f"---------------------------------------------------------------------------------")

        # sys.exit(0)

    except Exception as e:
        msg = f"ERROR: {e}"
        print(msg)
        # sys.exit(1)

    finally:
        pass


@emitter()
def cli_start(clo: Clo):
    broker_name = clo.get("name")
    broker_port = clo.get("port_spec")
    host_volume = clo.get("host_volume")
    max_message_size = clo.get("max_message_size")
    log_group_prefix = clo.get("log_group_prefix")
    net_name = clo.get("net_name")
    net_driver = clo.get("net_driver")
    context_yaml_file = clo.get("context")
    start(broker_name, broker_port, net_name, net_driver, host_volume, max_message_size,
          log_group_prefix, context_yaml_file)


@emitter()
def cli_stop(clo: Clo):
    broker_name = clo.get("name")
    stop(broker_name)


@emitter()
def cli_do_next_job(clo: Clo):
    for_app = clo.get("for")
    broker_host = clo.get("broker")
    wait_secs = int(clo.get("wait"))
    do_next_job(for_app, broker_host, wait_secs)


@emitter()
def cli_do_jobs(clo: Clo):
    for_app = clo.get("for")
    broker_host = clo.get("broker")
    wait_secs = int(clo.get("wait"))
    do_jobs(for_app, broker_host, wait_secs)


@emitter()
def cli_kick_job(clo: Clo):
    for_app = clo.get("for")
    broker_host = clo.get("broker")
    job_id = clo.get("id")
    kick_count = clo.get("at_most")
    kick_job(for_app, broker_host, job_id=job_id, kick_count=kick_count)


@emitter()
def cli_stats(clo: Clo):
    for_app = clo.get("for")
    broker_host = clo.get("broker")
    job_id = clo.get("id")
    system_stats, tube_stats, job_stats = stats(for_app, broker_host, job_id=job_id)
    print(f"{broker_host}: {system_stats}")
    print(f"{for_app}: {tube_stats}")
    if job_id:
        print(f"job {job_id}: {tube_stats}")


@emitter()
def cli_nq(clo: Clo):
    for_app = clo.get("for")
    broker_host = clo.get("broker")
    job_type = clo.get("job_type")
    job_data = clo.get("job_data")
    job_name = clo.get("job_name")
    priority = int(clo.get("priority"))
    delay_secs = int(clo.get("delay_secs"))
    ttr = int(clo.get("ttr"))
    nq(for_app, job_type, broker_host, job_data, job_name, priority, delay_secs, ttr)


@emitter()
def cli_purge(clo: Clo):
    for_app = clo.get("for")
    the_broker = clo.get("broker")
    at_most = clo.get("at_most")
    in_state = clo.get("in_state")
    purge(for_app, the_broker, at_most, in_state)


@emitter()
def cli_peek(clo: Clo):
    for_app = clo.get("for")
    the_broker = clo.get("broker")
    job_id = clo.get("id")
    next_in_state = clo.get("in_state")
    job = peek(for_app, the_broker, job_id, next_in_state)
    print(yaml.dump(json.loads(job.body), allow_unicode=True, default_flow_style=False))
