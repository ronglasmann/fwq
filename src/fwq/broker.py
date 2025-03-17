import os

from gwerks import emitter, is_dev_environment
from gwerks.docker import DockerApp, DockerContext, DockerNetwork


def parse_broker_str(broker: str):
    broker_s = broker.split(":")
    if len(broker_s) == 1:
        broker_tpl = (broker_s[0], int(Broker.DEFAULT_PORT))
    else:
        broker_tpl = (broker_s[0], int(broker_s[1]))
    return broker_tpl


@emitter()
class Broker(DockerApp):

    DOCKERFILE_BASE = ""
    DOCKERFILE_BASE += "FROM python:bullseye\n"
    DOCKERFILE_BASE += "RUN apt-get update\n"
    DOCKERFILE_BASE += "RUN pip3 install --upgrade pip\n"
    DOCKERFILE_BASE += "RUN pip3 --no-cache-dir install --upgrade awscli\n"
    DOCKERFILE_BASE += "ARG AWS_DEFAULT_REGION\n"
    DOCKERFILE_BASE += "ARG AWS_CONTAINER_CREDENTIALS_RELATIVE_URI\n"
    DOCKERFILE_BASE += "ARG AWS_ACCESS_KEY_ID\n"
    DOCKERFILE_BASE += "ARG AWS_SECRET_ACCESS_KEY\n"
    DOCKERFILE_BASE += "RUN apt-get install beanstalkd\n"

    DOCKERFILE = DOCKERFILE_BASE
    DOCKERFILE += "RUN pip3 install fwq\n"

    DOCKER_IMAGE_NAME = "fwq_broker"

    DEFAULT_NAME = "fwq_broker"
    DEFAULT_PORT = "11300"
    DEFAULT = f"{DEFAULT_NAME}:{DEFAULT_PORT}"

    def __init__(self, app_name=DEFAULT_NAME, context=None):
        super().__init__(app_name, Broker.DOCKER_IMAGE_NAME)

        if not context:
            context = DockerContext({
                "dockerfile_str": Broker.DOCKERFILE,
            })
            if is_dev_environment():
                context.set('pass_through_cloud_creds', "aws")
        self._context = context

    # --------------------------------------------------------------------------- #
    # delegates to run
    def start(self, port_spec: str = DEFAULT_PORT, network: DockerNetwork = None,
              map_to_host_data_volume: str = None, max_message_size: int = 65535, log_group_prefix: str = None):

        self.stop()
        self.remove()

        self._context.build(self.get_image_name())

        port_mappings = []
        if port_spec == "^":
            host_port_open = True
            port = int(Broker.DEFAULT_PORT)
            port_mappings.append((port, port))
            print(f"port_mappings: {port_mappings}")
        elif port_spec.endswith("^"):
            host_port_open = True
            port = int(port_spec[:-1])
            port_mappings.append((port, port))
            print(f"port_mappings: {port_mappings}")
        else:
            host_port_open = False
            port = int(port_spec)

        volume_mappings = []
        if map_to_host_data_volume:
            if not os.path.exists(map_to_host_data_volume):
                print(f"makedirs: {map_to_host_data_volume}")
                os.makedirs(map_to_host_data_volume)
            volume_mappings.append((map_to_host_data_volume, "/data"))
            print(f"volume_mappings: {volume_mappings}")

        # set up the log group
        if not log_group_prefix:
            log_group_prefix = ""
        log_group_prefix += f"/{self.get_app_name()}_{port}"
        print(f"log_group_prefix: {log_group_prefix}")

        app_cmd = f"beanstalkd -l 0.0.0.0 -p {port} -b /data -V -z {max_message_size}"
        print(f"app_cmd: {app_cmd}")

        self.run(app_cmd, network=network, port_mappings=port_mappings, volume_mappings=volume_mappings,
                 log_group_prefix=log_group_prefix)
        print(f"Ready to receive:")
        if host_port_open:
            print(f"broker (external): {self.get_host_name()}:{port}")
        print(f"broker (internal): {self.get_app_name()}:{port}")

    # --------------------------------------------------------------------------- #
    # delegates to stop, remove
    def halt(self):
        self.stop()
        self.remove()
        print(f"Stopped!")
