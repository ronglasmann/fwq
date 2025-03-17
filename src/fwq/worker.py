import json
import traceback

from greenstalk import Client, Job, TimedOutError
from gwerks import emitter, uid

from fwq.broker import parse_broker_str, Broker
from fwq.logs import JobLogger, Sender
from fwq.constants import JOB_TYPE_WORKER_CONTROL, WORKER_CONTROL_ACTION_RETIRE


@emitter()
class Worker:
    def __init__(self, for_app, broker: str = Broker.DEFAULT, worker_id=None):
        if not worker_id:
            worker_id = uid()
        self._uid = worker_id
        self._for_app = for_app
        self._broker = broker

        broker_tpl = parse_broker_str(broker)
        self._client = Client(broker_tpl, use=for_app, watch=[for_app])
        print(f"Worker [{self._uid}] is connected to {broker} doing work for [{for_app}]")

        self._work_loop_running = False
        self._job_type_map = {
            JOB_TYPE_WORKER_CONTROL: "self._handle_worker_control"
        }

        self._job_logger = JobLogger(self._broker, self._uid)

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
        print(f"Worker [{self._uid}] waiting for job from Broker [{self._broker}.{the_tube}]")

        job_body, job_id = self._reserve_job(timeout=wait_for_job_secs)

        # no job, timeout expired
        if job_id is None:
            return

        if job_body is None:
            raise Exception(f"job_body is empty for job {job_id}")

        if "type" not in job_body:
            raise Exception(f"'type' not in job_body for job {job_id}")
        job_type = job_body['type']

        if "name" not in job_body:
            raise Exception(f"'name' not in job_body for job {job_id}")
        job_name = job_body['name']

        job_data = {}
        if 'data' in job_body:
            job_data = job_body['data']

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
    #         self._bs_client, self._broker = get_beanstalk_client(self._beanstalk_host, self._for_app)
    #     return self._bs_client
