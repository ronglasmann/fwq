"""'Fast Work Queue'"""
from gwerks.packaging import get_version
__version__ = get_version(__file__)


# TODO job dependencies
# TODO worker control may need to listen to a separate control channel, test it regardless
# TODO alerting hook and Slack implementation
# TODO baseline cli config that has runtime_env setting (and region?)

"""


from gwerks.docker import DockerContext
from fwq.the_broker import Broker
DockerContext({
    "dockerfile_str": Broker.DOCKERFILE_BASE + "COPY gwerks.tar.gz .\nCOPY fwq.tar.gz .\nRUN pip3 install gwerks.tar.gz\nRUN pip3 install fwq.tar.gz\n",
    "files": [["../gwerks/dist/gwerks-25.2.13.tar.gz", "gwerks.tar.gz"], ["dist/fwq-0.0.0.tar.gz", "fwq.tar.gz"]]
}).to_yaml_file("./fwq_broker_docker_context-Dev.example.yaml")

DockerContext(from_yaml_file="./fwq_broker_docker_context-Dev.example.yaml").build("fwq_dev_image", no_cache=False)

"""


# --------------------------------------------------------------------------- #


