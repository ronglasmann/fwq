import json

from greenstalk import Client, NotFoundError

from fwq.broker import Broker, parse_broker_str
from fwq.constants import JOB_START_PERSISTENCE_DELAY_IN_SECS, JOB_STATE_READY, JOB_STATE_BURIED, JOB_STATE_DELAYED


class WorkQ:
    def __init__(self, for_app: str, broker: str = Broker.DEFAULT):
        self._for_app = for_app
        self._broker = broker

        broker_tpl = parse_broker_str(broker)
        self._client = Client(broker_tpl, use=for_app, watch=[for_app])

        print(f"WorkQ {self._for_app} is connected to {self._broker}")

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
            job_name = _make_job_name(job_type, job_data)

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

    def peek(self, job_id=None, next_in_state=JOB_STATE_READY):
        job = None
        try:
            if job_id is not None:
                job = self._client.peek(int(job_id))
            elif next_in_state == JOB_STATE_READY:
                job = self._client.peek_ready()
            elif next_in_state == JOB_STATE_BURIED:
                job = self._client.peek_buried()
            elif next_in_state == JOB_STATE_DELAYED:
                job = self._client.peek_delayed()
        except NotFoundError:
            if job_id is not None:
                print(f"job {job_id} not found")
            else:
                print(f"job not found")
        return job

    def purge(self, at_most, in_state=JOB_STATE_READY):
        purged_count = 0
        while purged_count < int(at_most):
            job = self.peek(next_in_state=in_state)
            if job is None:
                break
            try:
                self._client.delete(job)
                purged_count += 1
                print(f"{in_state} job {job.id} deleted")
            except NotFoundError:
                print(f"job {job.id} not found")

    def touch(self, job_id):
        job = self._client.peek(int(job_id))
        self._client.touch(job)

    # def use(self, a_tube_name):
    #     self._use_tube_name = a_tube_name
    #     use_tube = make_tube_name(self._for_app, self._use_tube_name)
    #     self._client().use(use_tube)

    # def all_jobs_status(self, for_state=None):
    #     results = []
    #     j_rows = self._db.select_active_jobs(self._beanstalk_server_id)
    #     if j_rows is not None:
    #         for j_row in j_rows:
    #             job_status = self.job_status(make_sj_id(self._beanstalk_server_id, j_row['job_id']))
    #             if job_status is not None:
    #                 if for_state is None or job_status['job_state'] == for_state:
    #                     results.append(job_status)

        # return results

    def stats(self, for_job_id=None):
        job_stats = None
        if for_job_id is not None:
            try:
                job_stats = self._client.stats_job(int(for_job_id))
            except NotFoundError:
                print(f"job {for_job_id} not found")

        tube_stats = self._client.stats_tube(self._for_app)

        sys_stats = self._client.stats()

        return sys_stats, tube_stats, job_stats

    def stop(self):
        if self._client is not None:
            self._client.close()
            self._client = None
            # self._broker = None

    # def _client(self) -> Client:
    #     if self._bs_client is None:
    #         self._bs_client, self._broker = get_beanstalk_client(self._beanstalk_host, self._for_app)
    #         # the_tube = make_tube_name(self._for_app, self._use_tube_name)
    #         print(f"WorkQ {self._for_app} is connected to {self._beanstalk_host}")
    #     return self._bs_client


def _make_job_name(job_type: str, job_data):
    module = job_type
    try:
        pkg, module = job_type.rsplit(".", 1)
    except Exception:
        pass
    return f"{module}-{abs(hash(json.dumps(job_data))) % (10 ** 8)}"
