import time
from fwq.api import job_is_canceled

def test_job(job_info):

    broker_id = job_info['broker_id']
    app = job_info['app']
    job_id = job_info['job_id']
    job_data = job_info['job_data']

    print(f"RUNNING")
    print(f"job_id: {job_id}")
    print(f"job_data: {job_data}")

    iterations = int(job_data.get('iterations', '3'))
    sleep_secs = int(job_data.get('sleep_secs', '1'))

    count = 0
    while not job_is_canceled(broker_id, app, job_id):
        count += 1
        print(f"ITERATION {count} of {iterations}")
        if count >= iterations:
            break

        print(f"SLEEPING for {sleep_secs} sec(s)")
        time.sleep(sleep_secs)

    print(f"EXITING")

    return "{'some': 'info'}"
