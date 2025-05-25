
CONFIG_DEFAULT = {
    "broker_name": "fwq",
    "beanstalkd_addr": "0.0.0.0",
    "beanstalkd_port": "11300",
    # "beanstalkd_port_host": None,
    # "beanstalkd_binlog": "/data",
    "beanstalkd_binlog_host": None,
    "beanstalkd_max_message_size": "65535",
    "etcd_client_port": "2379",
    # "etcd_client_port_host": None,
    "etcd_peering_port": "2380",
    "etcd_data_volume_host": None,
    # "etcd_peering_port_host": None,
    # "aws_log_group_prefix": None,
    "docker_network_driver": "bridge"
}
BROKERS_LIST_DEFAULT = []


JOB_TYPE_WORKER_CONTROL = "worker_control"
WORKER_CONTROL_ACTION_RETIRE = "retire"

JOB_START_PERSISTENCE_DELAY_IN_SECS = 1

JOB_STATE_READY = "ready"
JOB_STATE_DELAYED = "delayed"
JOB_STATE_RESERVED = "reserved"
JOB_STATE_BURIED = "buried"
JOB_STATE_DONE = "done"
JOB_STATE_ALL = "all"

# FINAL_JOB_STATES = [JOB_STATE_DONE, JOB_STATE_BURIED]
# PEEK_NEXT_JOB_STATES = [JOB_STATE_READY, JOB_STATE_DELAYED, JOB_STATE_BURIED]

# STATS_TYPE_SYSTEM = "system"
# STATS_TYPE_TUBE = "tube"
# STATS_TYPE_JOB = "job"

# BROKER_CONFIG_FROM_JOB_DATA = '_fwq_broker_config'
# WORK_Q_CONFIG_FROM_JOB_DATA = '_fwq_work_q_config'
# WORKER_CONFIG_FROM_JOB_DATA = '_fwq_worker_config'

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

KEY_APP_NAME = "APP_NAME"
