import os

from gwerks import emitter
# from gwerks.docker import DockerNetwork

from gwerks.docker import DockerBase
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
    def __init__(self, config):

        # container_name, network: Network, listen_addr=, port=, data_volume_host=None, max_message_size=
        config['image_name'] = "maateen/docker-beanstalkd"
        if "port" not in config:
            config["port"] = "11300"
        super().__init__(config)

        self._addr = "0.0.0.0"
        if "listen_addr" in config:
            self._addr = config["listen_addr"]

        self._data_volume = "/data"
        if "data_volume" in config:
            self._data_volume = config["data_volume"]
        # self._data_volume = os.path.abspath(self._data_volume)

        self._data_volume_host = None
        if "data_volume_host" in config:
            self._data_volume_host = config["data_volume_host"]
        self._data_volume_host = f"{os.path.abspath(self._data_volume_host).replace("\\", "/")}"

        self._max_message_size = "65535"
        if "max_message_size" in config:
            self._max_message_size = config["max_message_size"]

        if self._data_volume_host:
            self._volume_mappings.append([self._data_volume_host, self._data_volume])

    def start(self):
        # self.get_docker_network().create()

        cmd = ""
        cmd += f"-V "
        cmd += f"-b {self._data_volume} "
        if self._addr:
            cmd += f"-l {self._addr} "
        if self._port:
            cmd += f"-p {self._port} "
        if self._max_message_size:
            cmd += f"-z {self._max_message_size} "

        self.docker_run(cmd)

    def stop(self):
        self.docker_stop()
