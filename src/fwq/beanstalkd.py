from gwerks import emitter

from fwq.docker import DockerBase, Network
import fwq.key

from greenstalk import Client, NotFoundError
from tenacity import Retrying, stop_after_delay, wait_fixed

_clients = {}


def get_stats(broker_id, app, job_id=None):
    beanstalk_client = get_beanstalk_client(broker_id, app)

    job_stats = None
    if job_id is not None:
        try:
            job_stats = beanstalk_client.stats_job(int(job_id))
        except NotFoundError:
            print(f"job {job_id} not found")

    tube_stats = beanstalk_client.stats_tube(app)

    sys_stats = beanstalk_client.stats()

    return sys_stats, tube_stats, job_stats


# @emitter()
def get_beanstalk_client(broker_id, app, reconnect=False, timeout_secs=10, retry_secs=1):
    # fwq_nfo = fwq.key.parser(key_str)
    client = _clients.get(broker_id, None)
    if not client or reconnect is True:
        for attempt in Retrying(reraise=True, stop=stop_after_delay(timeout_secs), wait=wait_fixed(retry_secs)):
            with attempt:
                broker_nfo = fwq.key.broker_id_parser(broker_id)
                host = broker_nfo.host
                beanstalk_port = broker_nfo.beanstalk_port
                print(f"Attempting Beanstalkd connection: {host}_{beanstalk_port}_{app}...")
                client = Client((host, beanstalk_port), use=app, watch=[app])
        _clients[broker_id] = client
    return client


@emitter()
class Beanstalkd(DockerBase):
    def __init__(self, container_name, network: Network, listen_addr="0.0.0.0", port="11300", data_volume_host=None,
                 max_message_size="65535"):
        super().__init__(container_name, "maateen/docker-beanstalkd", network)

        self._addr = listen_addr
        self._port = port

        self._data_volume = "/data"
        self._data_volume_host = data_volume_host

        self._max_message_size = max_message_size

        self._published_ports.append(port)
        if self._data_volume_host:
            self._volume_mappings.append([self._data_volume_host, self._data_volume])

    def get_port(self):
        return self._port

    def start(self):
        self._docker_network_create()

        cmd = ""
        cmd += f"-V "
        cmd += f"-b {self._data_volume} "
        if self._addr:
            cmd += f"-l {self._addr} "
        if self._port:
            cmd += f"-p {self._port} "
        if self._max_message_size:
            cmd += f"-z {self._max_message_size} "

        self._docker_run(cmd)

    def stop(self):
        self._docker_stop()
