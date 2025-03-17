import time


def test_job(job_info):

    job_id = job_info['job_id']
    job_data = job_info['job_data']

    print(f"test_job RUNNING")
    print(f"job_id: {job_id}")
    print(f"job_data: {job_data}")

    print(f"test_job SLEEPING")
    time.sleep(5)

    print(f"I'm awake!")

    return "{'some': 'info'}"
