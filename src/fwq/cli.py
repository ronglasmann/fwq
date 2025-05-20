import os
import sys
import yaml
import json
import argparse

from gwerks.decorators import emitter
# from gwerks.cli import cli, Clo

from fwq.constants import JOB_STATE_READY

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
def cli_fwq():

    _debug_traceback_limit = 1000
    sys.tracebacklimit = 0

    # Create an argument parser object
    parser = argparse.ArgumentParser(description="Fast Work Queue")

    # Add arguments
    parser.add_argument("component", help="[config | broker | job | worker]")
    parser.add_argument("action", help="the component action")
    parser.add_argument("-d", "--debug", help="activate debug mode", action="store_true")
    parser.add_argument("-s", "--spec", help="component (config | job) specification")
    parser.add_argument("-i", "--id", help="component (broker | job | worker) identifier")
    parser.add_argument("-v", "--value", help="component value for identifier")
    parser.add_argument("-a", "--app", help="job target application")
    parser.add_argument("-t", "--state", help="job state")
    parser.add_argument("-c", "--count", help="job count")
    parser.add_argument("-w", "--wait", help="wait time in seconds", default="60")

    # Parse the arguments from the command line
    args = parser.parse_args()

    try:

        if args.debug:
            sys.tracebacklimit = _debug_traceback_limit

        action = f"{args.component}_{args.action}"
        cli_action = f"cli_{action}"

        print(f"---------------------------------------------------------------------------------")

        globals()[cli_action](args)

        print(f"---------------------------------------------------------------------------------")

        # sys.exit(0)

    except Exception as e:
        msg = f"ERROR: {e}"
        print(msg)
        # sys.exit(1)

    finally:
        pass


@emitter()
def cli_job_kick(args):
    from fwq.key import parser
    from fwq.api import job_kick
    job_count = args.count
    nfo = parser(args.id)
    job_kick(nfo.broker_id, nfo.app, job_id=nfo.job_id, job_count=job_count)


@emitter()
def cli_job_touch(args):
    from fwq.key import parser
    from fwq.api import job_touch
    nfo = parser(args.id)
    job_touch(nfo.broker_id, nfo.app, nfo.job_id)


@emitter()
def cli_job_cancel(args):
    from fwq.key import parser
    from fwq.api import job_cancel
    nfo = parser(args.id)
    job_cancel(nfo.broker_id, nfo.app, nfo.job_id)


@emitter()
def cli_job_purge(args):
    from fwq.key import parser
    from fwq.api import job_purge, job_purge_completed
    older_than_secs = args.older_than
    nfo = parser(args.id)
    if nfo.job_id:
        job_purge(nfo.broker_id, nfo.app, nfo.job_id)
    else
        job_purge_completed(nfo.broker_id, nfo.app, older_than_secs)

@emitter()
def cli_job_do_all(args):
    from fwq.key import parser
    from fwq.api import job_do_all
    wait_secs = args.wait
    nfo = parser(args.id)
    job_do_all(nfo.broker_id, nfo.app, wait_secs)


@emitter()
def cli_job_do_next(args):
    from fwq.key import parser
    from fwq.api import job_do_next
    wait_secs = args.wait
    nfo = parser(args.id)
    job_do_next(nfo.broker_id, nfo.app, wait_secs)


@emitter()
def cli_job_list(args):
    from fwq.key import parser
    from fwq.api import job_list
    nfo = parser(args.id)
    job_states = None  # meaning all states
    job_states_str = args.state
    if job_states_str:
        job_states = job_states_str.split(",")
    print(f"jobs: {job_list(nfo.broker_id, nfo.app, job_states=job_states)}")


@emitter()
def cli_job_get(args):
    from fwq.key import parser
    from fwq.api import job_get
    nfo = parser(args.id)
    print(f"job: {job_get(nfo.broker_id, nfo.app)}")


@emitter()
def cli_job_nq(args):
    from fwq.api import job_nq
    job_spec = json.loads(args.spec)
    print(f"job_id: {job_nq(job_spec)}")


@emitter()
def cli_broker_start(args):
    from fwq.api import broker_start
    print(f"broker nfo: {broker_start()}")
    # print(f"DockerNet: {beanstalkd.get_container_name()}_{beanstalkd.get_port()}_{etcd.get_client_port()}")
    # print(f"HostNet:   {beanstalkd.get_host_ip()}_{beanstalkd.get_port()}_{etcd.get_client_port()}")



@emitter()
def cli_broker_stop(args):
    from fwq.api import broker_stop
    broker_stop()


@emitter()
def cli_broker_add(args):
    from fwq.api import broker_add
    broker_id = args.id
    print(f"brokers: {broker_add(broker_id)}")


@emitter()
def cli_broker_rm(args):
    from fwq.api import broker_rm
    broker_id = args.id
    print(f"brokers: {broker_rm(broker_id)}")


@emitter()
def cli_broker_list(args):
    from fwq.api import broker_list
    print(f"brokers: {broker_list()}")


@emitter()
def cli_config_set(args):
    from fwq.api import config_set
    config_str = args.spec
    config_item_key = args.id
    config_item_value = args.value
    config = config_set(config_str=config_str, config_item_key=config_item_key, config_item_value=config_item_value)
    print(f"config: {config}")


@emitter()
def cli_config_get(args):
    from fwq.api import config_get
    config = config_get()
    print(f"config: {config}")


def cli_config_reset(args):
    from fwq.api import config_reset
    config = config_reset()
    print(f"config: {config}")
