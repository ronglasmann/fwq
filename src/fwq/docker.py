import os
import socket

from gwerks import environment, is_dev_environment
from gwerks.util.sys import exec_cmd


class Network:
    def __init__(self, name, driver="bridge"):
        self._name = name
        self._driver = driver

    def get_name(self):
        return self._name

    def get_driver(self):
        return self._driver


# @emitter()
class DockerBase:
    def __init__(self, container_name, image_name, network: Network, run_as = (os.getuid(), os.getgid())):
        self._container_name = container_name
        self._image_name = image_name
        self._network = network
        self._run_as = run_as

        self._published_ports = []
        self._volume_mappings = []

        self._host_name = socket.gethostname()
        self._host_ip = socket.gethostbyname(self._host_name)

    def get_network(self):
        return self._network

    def get_host_ip(self):
        return self._host_ip

    def get_container_name(self):
        return self._container_name

    def _exec(self, cmd):
        return exec_cmd(cmd, no_sudo=is_dev_environment(), return_tuple=True)

    def _docker_stop(self):
        cmd = ""
        cmd += f"docker rm --force {self._container_name} "
        return self._exec(cmd)

    def _docker_run(self, cmd_line=None, env_vars=None):
        cmd = ""
        cmd += f"docker run -d --name {self._container_name} "
        cmd += f"--env RUNTIME_ENV={environment()} "
        cmd += f"--env PYTHONUNBUFFERED=1 "
        if env_vars:
            cmd += f"{env_vars} "
        cmd += self._docker_run_env_dev_aws_keys()
        if self._network:
            cmd += f"--network {self._network.get_name()} "
        for p in self._published_ports:
            cmd += f"--publish {p}:{p} "
        for v_map in self._volume_mappings:
            os.makedirs(v_map[0], exist_ok=True)
            cmd += f"--volume {v_map[0]}:{v_map[1]} "
        cmd += self._docker_run_log_driver()
        cmd += f"--user {self._run_as[0]}:{self._run_as[1]} "
        cmd += f"{self._image_name} "
        if cmd_line:
            cmd += f"{cmd_line} "
        return self._exec(cmd)

    def _docker_run_log_driver(self):
        cmd = ""
        # use the aws log driver in Test and Live so the logs go to Cloudwatch
        if not is_dev_environment():
            log_group = f"{self._network.get_name()}/{environment()}/{self._host_name}"
            if not log_group.startswith("/"):
                log_group = "/" + log_group
            if log_group.endswith("/"):
                log_group = log_group[:-1]
            cmd += f"--log-driver=awslogs "
            cmd += f"--log-opt awslogs-group={log_group} --log-opt awslogs-create-group=true "
            cmd += f"--log-opt awslogs-stream={self._container_name} "
        return cmd

    def _docker_run_env_dev_aws_keys(self):
        cmd = ""
        # in the Dev environment_name expect AWS keys must be set in the system environment_name
        if is_dev_environment():
            cmd += f"--env AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID "
            cmd += f"--env AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY "
        return cmd

    def _docker_network_create(self):
        cmd = ""
        cmd += f"docker network inspect {self._network.get_name()} >/dev/null 2>&1 || "
        cmd += f"docker network create --driver {self._network.get_driver()} {self._network.get_name()}"
        # cmd = f"docker network create {self._network.get_name()} --driver {self._network.get_driver()}"
        return self._exec(cmd)


