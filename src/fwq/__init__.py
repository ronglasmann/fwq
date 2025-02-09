"""'Fast Work Queue'"""
import json
import sys
import traceback
from contextlib import contextmanager

from greenstalk import Client, Job, TimedOutError, NotFoundError

from gwerks.docker import *

from fwq.logs import JobLogger, Sender
from fwq.constants import *

from . import _version
__version__ = _version.get_versions()['version']

# TODO job dependencies
# TODO worker control may need to listen to a separate control channel, test it regardless
# TODO cli for starting a broker
# TODO cli for opening a WorkQ
# TODO cli for touching jobs
# TODO cli for using tubes
# TODO alerting hook and Slack implementation
# TODO baseline cli config that has runtime_env setting (and region?)

# this is used by the build process as the package version
# __version__ = get_version(__file__)


# --------------------------------------------------------------------------- #

class Environment:
    def __init__(self):
        pass


class Broker(DockerApp):
    def __init__(self, app_name="fwq_broker", docker_image="fwq-broker",
                 broker_port=11300,
                 map_to_host_port=None,
                 map_to_host_data_volume="data",
                 max_message_size=65535,
                 log_group_prefix=None):

        self._broker_port = broker_port
        self._max_message_size = max_message_size

        # configure the DockerApp super class
        app_cmd = f"beanstalkd -l 0.0.0.0 -p {self._broker_port} -b /data -V -z {self._max_message_size}"
        super().__init__(app_name, app_cmd, docker_image)

        # port and volume mappings to the Docker host
        self._map_to_host_port = map_to_host_port
        self._map_to_host_data_volume = map_to_host_data_volume
        if self._map_to_host_port is not None:
            self._port_mappings.append((self._map_to_host_port, self._broker_port))
        if self._map_to_host_data_volume is not None:
            self._volume_mappings.append((self._map_to_host_data_volume, "/data"))

        # set up the log group
        self._log_group_base = ""
        if log_group_prefix is not None:
            self._log_group_base += f"/{log_group_prefix}"
        self._log_group_base += f"/{self.get_app_name()}_{self._broker_port}"

        self._broker_id = None

        # get ready to go
        if is_dev_environment():
            module_path = os.path.dirname(__file__)
            build_context_path = os.path.join(module_path, "../..")
            self.build(build_context_path, pass_aws_creds_to_image=True)
        else:
            self.pull()

    # def set_up(self):
    #     if is_dev_environment():
    #         self.build(pass_aws_creds_to_image=True)
    #     else:
    #         self.pull()
        # docker_network_create(driver=self._docker_net_driver, net_name=self._docker_net_name)
        # if is_dev_environment():
        #     docker_prune()
        #     docker_login_ecr(profile=aws_profile, ecr_repo=self._ecr_repo)
        #     docker_pull(docker_image_name=self._docker_image)
        # else:
        #     docker_service_start()
        #     docker_prune()
        #     docker_login_ecr(ecr_repo=self._ecr_repo)
        #     docker_pull(docker_image_name=self._docker_image)

    # def start(self):
    #     self.run()
        # cmd = f"beanstalkd -l 0.0.0.0 -p {self._broker_port} -b /data -V -z {self._max_message_size}"
        # docker_run(self._broker_addr, cmd, docker_image_name=self._docker_image,
        #            net_name=self._docker_net_name, volume_mappings=volume_mappings, port_mappings=port_mappings,
        #            log_group_base=self._log_group_base)

    def get_host_client(self, for_app) -> Client:
        if self._map_to_host_port is None:
            raise Exception(f"host port mapping is none, unable to create a host client")
        broker_host = (self.get_host_addr(), self._map_to_host_port)
        # the_tube = for_app
        # client = Client(broker_host, use=the_tube, watch=[the_tube])
        # sys_stats = client.stats()
        # self._broker_id = sys_stats['id']
        return self._get_client(for_app, broker_host)

    def get_docker_net_client(self, for_app) -> Client:
        broker_host = (self.get_app_name(), self._broker_port)
        # the_tube = for_app
        # client = Client(self._broker_addr, use=the_tube, watch=[the_tube])
        # sys_stats = client.stats()
        # self._broker_id = sys_stats['id']
        return self._get_client(for_app, broker_host)

    def _get_client(self, for_app, broker_host) -> Client:
        the_tube = for_app
        client = Client(broker_host, use=the_tube, watch=[the_tube])
        sys_stats = client.stats()
        self._broker_id = sys_stats['id']
        return client

    def get_id(self):
        return self._broker_id

    # def stop(self):
        # docker_stop(self._broker_addr)

    # def take_down(self):
    #     self.remove()
        # docker_remove(self._broker_addr)
        # docker_network_destroy(net_name=self._docker_net_name)


class Worker:
    def __init__(self, for_app, broker: Broker, worker_id=None, connect_to_broker_host=False):

        # self._config = config
        self._uid = worker_id
        self._for_app = for_app
        # self._beanstalk_host = config["broker_addr"]
        # self._use_tube_name = config["tube_name"]

        self._broker = broker
        if connect_to_broker_host:
            self._client = broker.get_host_client(for_app)
        else:
            self._client = broker.get_docker_net_client(for_app)
        print(f"Worker {self._for_app} is connected to {self._broker.get_id()}")

        self._work_loop_running = False
        # self._bs_client: [Client, None] = None
        # self._broker_id = None
        self._job_type_map = {
            JOB_TYPE_WORKER_CONTROL: "self._handle_worker_control"
        }

        self._job_logger = JobLogger(self._broker.get_id(), self._uid)

    def register_log_sender(self, log_sender: Sender):
        self._job_logger.register_logstream(log_sender)

    def do_jobs(self, wait_for_job_secs=60):
        self._work_loop_running = True
        while self._work_loop_running:
            try:
                self.do_job(wait_for_job_secs)

            except Exception as e:
                traceback.print_exc()
                print(f"Unexpected ERROR in work loop: {e}")

    def do_job(self, wait_for_job_secs=60):

        # the_tube = make_tube_name(self._for_app, self._use_tube_name)
        the_tube = self._for_app
        print(f"Worker [{self._uid}] waiting for job from Broker [{self._broker.get_id()}.{the_tube}]")

        job_body, job_id = self._reserve_job(timeout=wait_for_job_secs)

        # no job, timeout expired
        if job_id is None:
            return

        if job_body is None:
            raise Exception(f"job_body is empty for job {job_id}")

        if "type" not in job_body:
            raise Exception(f"'type' not in job_body for job {job_id}")

        job_type = job_body['type']

        job_data = {}
        if 'data' in job_body:
            job_data = job_body['data']

        if 'name' in job_body:
            job_name = job_body['name']
        else:
            job_name = make_job_name(job_type, job_data)

        # with JobPrint(self._beanstalk_server_id, self._uid, job_id):
        self._job_logger.change_jobs(job_id)
        with self._job_logger:
            try:

                print(f"---------------------------------------------------------------------------------")
                print(f"{job_name} is STARTING")

                if job_type in self._job_type_map.keys():
                    job_type = self._job_type_map[job_type]
                print(f"job_type: {job_type}")
                print(f"job_data: {job_data}")

                code = ""
                try:
                    job_type = str(job_type)
                    ri = job_type.rindex(".")
                    code += f"from {job_type[:ri]} import {job_type[ri + 1:]}; " \
                            f"globals()['result'] = {job_type[ri + 1:]}(job_info)"
                except ValueError:
                    code += f"globals()['result'] = {job_type}(job_info)"

                print(f"exec: {code}")

                print(f"---------------------------------------------------------------------------------")
                print(f"{job_name} is RUNNING")

                g = {"job_info": {"job_data": job_data, "job_id": job_id}}
                exec(code, g)
                result = g['result']

                print(f"---------------------------------------------------------------------------------")
                print(f"{job_name} is EXITING")
                print(f"result: {result}")

                if result is not None and result is False:
                    self._client.release(Job(job_id, job_body))
                    print(f"resolution: not handled and released")

                else:
                    self._client.delete(job_id)
                    print(f"resolution: succeeded and deleted")

            except Exception as e:
                result = {"error": str(e)}
                print(f"---------------------------------------------------------------------------------")
                print(f"{job_name} is EXITING")
                print(f"result: {result}")
                try:
                    self._client.bury(Job(job_id, job_body))
                    print(f"resolution: failed and buried")
                except Exception as ex:
                    print(f"ERROR when burying job ({job_id}) {job_name}")

            finally:
                print(f"---------------------------------------------------------------------------------")

    def _reserve_job(self, timeout=None):
        try:
            gs_job = self._client.reserve(timeout)
            job_id = gs_job.id
            job_body = json.loads(gs_job.body)
            if "type" not in job_body.keys():
                raise Exception(f"'type' not found in the job body for {gs_job} from {self._for_app}")
            # if "data" not in job_body.keys():
            #     raise Exception(f"'data' not found in the job body for {gs_job} from {self._for_app}")
            # if "name" not in job_body.keys():
            #     raise Exception(f"'name' not found in the job body for {gs_job} from {self._for_app}")
            # job_type = job_body['type']
            # job_data = job_body['data']
            return job_body, job_id
        except TimedOutError:
            return None, None

    def _handle_worker_control(self, job_data):
        worker_id = job_data['worker_id']
        if worker_id != self._uid:
            print(f"Specified worker_id ({worker_id}) does not match this worker ({self._uid}), "
                  f"job is for a different worker")
            return False
        action = job_data['action']
        if action == WORKER_CONTROL_ACTION_RETIRE:
            print(f"Worker [{self._uid}] has been directed to retire")
            self._work_loop_running = False

        else:
            raise Exception(f"Unsupported worker control action: {action}")

    # def _client(self) -> Client:
    #     if self._bs_client is None:
    #         self._bs_client, self._broker_id = get_beanstalk_client(self._beanstalk_host, self._for_app)
    #     return self._bs_client


class WorkQ:
    def __init__(self, for_app, broker: Broker, connect_to_broker_host=False):

        # self._config = config
        self._for_app = for_app
        # self._beanstalk_host = config["broker_addr"]
        # self._use_tube_name = config["tube_name"]

        # self._bs_client: [Client, None] = None
        self._broker = broker
        if connect_to_broker_host:
            self._client = broker.get_host_client(for_app)
        else:
            self._client = broker.get_docker_net_client(for_app)
        print(f"WorkQ {self._for_app} is connected to {self._broker.get_id()}")

    def kick(self, job_id=None, how_many=None):
        number_kicked = 0
        try:
            if job_id is not None:
                self._client.kick_job(int(job_id))
                number_kicked += 1
            elif how_many is not None:
                number_kicked = self._client.kick(int(how_many))
        except NotFoundError:
            if job_id is not None:
                print(f"job {job_id} not found")
            else:
                print(f"job not found")
        return number_kicked

    def put(self, job_type, job_data=None, job_name=None, priority=65536, delay=0, ttr=60):

        if job_name is None:
            job_name = make_job_name(job_type, job_data)

        if job_data is None:
            job_data = {}
        # job_data['_fwq_work_q_config'] = json.dumps(self._config)  # TODO fix

        job_body = json.dumps({
            "type": job_type,
            "data": job_data,
            "name": job_name
        })
        job_id = self._client.put(job_body, priority=priority, delay=delay + JOB_START_PERSISTENCE_DELAY_IN_SECS,
                                    ttr=ttr)
        print(f"NQ Job [{job_id}] - {job_name} - [priority={priority}, delay={delay}, ttr={ttr}]")
        return job_id

    def peek(self, for_id=None, next_for_state=JOB_STATE_READY):
        job = None
        try:
            if for_id is not None:
                job = self._client.peek(int(for_id))
            elif next_for_state == JOB_STATE_READY:
                job = self._client.peek_ready()
            elif next_for_state == JOB_STATE_BURIED:
                job = self._client.peek_buried()
            elif next_for_state == JOB_STATE_DELAYED:
                job = self._client.peek_delayed()
        except NotFoundError:
            if for_id is not None:
                print(f"job {for_id} not found")
            else:
                print(f"job not found")
        return job

    def purge(self, how_many, for_state=JOB_STATE_READY):
        purged_count = 0
        while purged_count < int(how_many):
            job = self.peek(next_for_state=for_state)
            if job is None:
                break
            try:
                self._client.delete(job)
                purged_count += 1
                print(f"{for_state} job {job.id} deleted")
            except NotFoundError:
                print(f"job {job.id} not found")

    def touch(self, job_id):
        job = self._client.peek(int(job_id))
        self._client.touch(job)

    # def use(self, a_tube_name):
    #     self._use_tube_name = a_tube_name
    #     use_tube = make_tube_name(self._for_app, self._use_tube_name)
    #     self._client().use(use_tube)

    def stats(self, for_tube=None, for_job_id=None):
        job_stats = None
        if for_job_id is not None:
            try:
                job_stats = self._client.stats_job(int(for_job_id))
            except NotFoundError:
                print(f"job {for_job_id} not found")

        tube_stats = {}
        if for_tube is None:
            tubes = self._client.tubes()
            for tube_name in tubes:
                tube_stats[tube_name] = self._client.stats_tube(tube_name)
        else:
            tube_name = self._for_app
            tube_stats[tube_name] = self._client.stats_tube(tube_name)

        sys_stats = self._client.stats()
        return sys_stats, tube_stats, job_stats

    def stop(self):
        if self._client is not None:
            self._client.close()
            self._client = None
            # self._broker_id = None

    # def _client(self) -> Client:
    #     if self._bs_client is None:
    #         self._bs_client, self._broker_id = get_beanstalk_client(self._beanstalk_host, self._for_app)
    #         # the_tube = make_tube_name(self._for_app, self._use_tube_name)
    #         print(f"WorkQ {self._for_app} is connected to {self._beanstalk_host}")
    #     return self._bs_client


@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout


def make_job_name(job_type: str, job_data):
    module = job_type
    try:
        pkg, module = job_type.rsplit(".", 1)
    except Exception:
        pass
    return f"{module}-{abs(hash(json.dumps(job_data))) % (10 ** 8)}"


# def make_tube_name(for_app, tube_name):
#     base_tube = f"{environment()}_{for_app}"
#     use_tube = f"{base_tube}_{tube_name}"
#     return use_tube

