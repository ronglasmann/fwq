from fwq.broker import Broker
from fwq.worker import Worker
from fwq.work_q import WorkQ
from fwq.constants import JOB_STATE_READY

from gwerks.docker import DockerNetwork, DockerContext


def start(name=Broker.DEFAULT_NAME, port=Broker.DEFAULT_PORT, net_name=None, net_driver=DockerNetwork.DEFAULT_DRIVER,
          host_volume=None, max_message_size=65535, log_group_prefix=None, context_yaml_file=None):
    network = None
    if net_name:
        network = DockerNetwork(net_name, driver=net_driver)

    context = None
    if context_yaml_file:
        context = DockerContext(from_yaml_file=context_yaml_file)

    the_broker = Broker(app_name=name, context=context)

    the_broker.start(port_spec=port, network=network, map_to_host_data_volume=host_volume,
                     max_message_size=max_message_size, log_group_prefix=log_group_prefix)


def stop(name=Broker.DEFAULT_NAME):
    the_broker = Broker(app_name=name)
    the_broker.halt()


def do_next_job(for_app, the_broker=f"{Broker.DEFAULT_NAME}:{Broker.DEFAULT_PORT}", wait_secs=60):
    the_worker = Worker(for_app, the_broker)
    the_worker.do_job(wait_for_job_secs=wait_secs)


def do_jobs(for_app, the_broker=f"{Broker.DEFAULT_NAME}:{Broker.DEFAULT_PORT}", wait_secs=60):
    the_worker = Worker(for_app, the_broker)
    the_worker.do_jobs(wait_for_job_secs=wait_secs)


def kick_job(for_app, the_broker=f"{Broker.DEFAULT_NAME}:{Broker.DEFAULT_PORT}", job_id=None, kick_count=None):
    the_work_q = WorkQ(for_app, the_broker)
    the_work_q.kick(job_id=job_id, how_many=kick_count)


def stats(for_app, the_broker=f"{Broker.DEFAULT_NAME}:{Broker.DEFAULT_PORT}", job_id=None):
    the_work_q = WorkQ(for_app, the_broker)
    system_stats, tube_stats, job_stats = the_work_q.stats(for_job_id=job_id)
    return system_stats, tube_stats, job_stats


def nq(for_app, job_type, the_broker=f"{Broker.DEFAULT_NAME}:{Broker.DEFAULT_PORT}", job_data=None, job_name=None,
       priority=65536, delay_secs=0, ttr=60):
    the_work_q = WorkQ(for_app, the_broker)
    the_work_q.put(job_type, job_data, job_name, priority, delay_secs, ttr)


def purge(for_app, the_broker=f"{Broker.DEFAULT_NAME}:{Broker.DEFAULT_PORT}", at_most=None, in_state=JOB_STATE_READY):
    the_work_q = WorkQ(for_app, the_broker)
    the_work_q.purge(at_most=at_most, in_state=in_state)


def peek(for_app, the_broker=f"{Broker.DEFAULT_NAME}:{Broker.DEFAULT_PORT}", job_id=None, next_in_state=JOB_STATE_READY):
    the_work_q = WorkQ(for_app, the_broker)
    return the_work_q.peek(job_id=job_id, next_in_state=next_in_state)
