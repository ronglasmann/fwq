import json
import traceback

from greenstalk import Job, TimedOutError
from gwerks import emitter, uid

from fwq.logs import JobLogger, Sender
from fwq.constants import JOB_TYPE_WORKER_CONTROL, WORKER_CONTROL_ACTION_RETIRE, JOB_STATE_RESERVED, JOB_STATE_BURIED, \
    JOB_STATE_DONE, JOB_STATE_READY
import fwq.key
import fwq.beanstalkd
import fwq.etcd


# @emitter()
class Worker:
    def __init__(self, broker_id, app, worker_id=None):
        if not worker_id:
            worker_id = uid()
        self._uid = worker_id
        # self._key_str = key_str
        self._broker_id = broker_id
        self._app = app

        self._beanstalk_client = fwq.beanstalkd.get_beanstalk_client(self._broker_id, self._app)
        self._etcd_client = fwq.etcd.get_etcd_client(self._broker_id)

        # fwq_nfo = fwq.key.parser(key_str)
        # self._broker_id = fwq_nfo.broker_id
        # self._app = fwq_nfo.app
        self._job_logger = JobLogger(self._broker_id, self._uid)

        # print(f"Worker [{self._uid}] is connected to {self._broker_id} doing work for [{self._app}]")

        self._work_loop_running = False
        self._job_type_map = {
            JOB_TYPE_WORKER_CONTROL: "self._handle_worker_control"
        }

    def register_log_sender(self, log_sender: Sender):
        self._job_logger.register_logstream(log_sender)

    def do_jobs(self, wait_for_job_secs=60, keep_waiting=True):
        self._work_loop_running = True
        while self._work_loop_running:
            self._work_loop_running = keep_waiting
            # print(f"Worker [{self._uid}] is connected to {self._broker_id} doing work for [{self._app}]")
            while True:
                try:
                    job_attempted = self.do_job(wait_for_job_secs)
                    if not job_attempted:
                        break
                except BrokenPipeError as bpe:
                    print(f"BrokenPipeError in work loop: {bpe}, reconnecting....")
                    self._beanstalk_client = fwq.beanstalkd.get_beanstalk_client(self._broker_id, self._app, reconnect=True)
                    break
                except Exception as e:
                    print(f"Unexpected ERROR: {e}, work loop will exit")
                    traceback.print_exc()
                    self._work_loop_running = False
                    break

    def do_job(self, wait_for_job_secs=60):

        # the_tube = make_tube_name(self._for_app, self._use_tube_name)
        print(f"Worker [{self._uid}] waiting for job from Broker /{self._broker_id}/{self._app}]")

        job_body, job_id = self._reserve_job(timeout=wait_for_job_secs)
        job_name = "UNKNOWN"

        # no job, timeout expired
        if job_id is None:
            return False

        # key_prefix = f"/{self._broker_id}/{self._app}"

        try:
            self._etcd_client.update_job_state(self._app,  job_id, JOB_STATE_RESERVED)

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

            self._job_logger.change_jobs(job_id)
            with self._job_logger:

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

                g = {"job_info": {"broker_id":self._broker_id, "app": self._app, "job_data": job_data, "job_id": job_id}}
                exec(code, g)
                result = g['result']

                print(f"---------------------------------------------------------------------------------")
                print(f"{job_name} is EXITING")
                print(f"result: {result}")

                if result is not None and result is False:
                    self._etcd_client.update_job_state(self._app, job_id, JOB_STATE_READY)
                    # self._etcd_client.update(job_key, "state", JOB_STATE_READY)
                    self._beanstalk_client.release(Job(job_id, job_body))
                    print(f"resolution: not handled and released")

                else:
                    self._etcd_client.update_job_state(self._app, job_id, JOB_STATE_DONE)
                    # self._etcd_client.update(job_key, "state", JOB_STATE_DONE)
                    self._beanstalk_client.delete(job_id)
                    print(f"resolution: succeeded and deleted")

        except Exception as e:
            result = {"error": str(e)}
            print(f"---------------------------------------------------------------------------------")
            print(f"{job_name} is EXITING")
            print(f"result: {result}")
            try:
                self._etcd_client.update_job_state(self._app, job_id, JOB_STATE_BURIED)
                # self._etcd_client.update(job_key, "state", JOB_STATE_BURIED)
                self._beanstalk_client.bury(Job(job_id, job_body))
                print(f"resolution: failed and buried")
            except Exception as ex:
                print(f"ERROR when burying job ({job_id}) {job_name}")

        finally:
            print(f"---------------------------------------------------------------------------------")
            return True

    def _reserve_job(self, timeout=None):
        try:
            gs_job = self._beanstalk_client.reserve(timeout)
            job_id = gs_job.id
            job_body = json.loads(gs_job.body)
            # if "type" not in job_body.keys():
            #     raise Exception(f"'type' not found in the job body for {gs_job} from {self._app}")
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
