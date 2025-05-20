from types import SimpleNamespace

def maker(broker_id, app, job_id):
    key = ""
    if not broker_id:
        return key
    key += "/" + broker_id
    if not app:
        return key
    key += "/" + app
    if not job_id:
        return key
    key += "/" + job_id
    return key

def parser(key_str, key_type="detail"):
    broker_id = None
    broker = None
    app = None
    job_id = None
    state = None
    job_nfo = key_str.split('/')
    if len(job_nfo) > 1:
        broker_id = job_nfo[1]
        broker = broker_id_parser(broker_id)
    if len(job_nfo) > 2:
        app = job_nfo[2]

    if key_type == "detail":
        if len(job_nfo) > 3:
            job_id = job_nfo[3]
    else:
        if len(job_nfo) > 4:
            state = job_nfo[4]
        if len(job_nfo) > 5:
            job_id = job_nfo[5]

    # print(f"job_key: {job_key}")
    # print(f"broker_id: {broker_id}")
    # print(f"app: {app}")
    # print(f"job_id: {job_id}")
    return SimpleNamespace(broker_id=broker_id, broker=broker, app=app, job_id=job_id, state=state)

def broker_id_maker(host, beanstalk_port, etcd_port):
    broker_id = ""
    if not host:
        return broker_id
    broker_id += host
    if not beanstalk_port:
        return broker_id
    broker_id += "_" + beanstalk_port
    if not etcd_port:
        return broker_id
    broker_id += "_" + etcd_port
    return broker_id

def broker_id_parser(broker_id):
    host = None
    beanstalk_port = None
    etcd_port = None
    broker_nfo = broker_id.split('_')
    if len(broker_nfo) > 0:
        host = broker_nfo[0]
    if len(broker_nfo) > 1:
        beanstalk_port = broker_nfo[1]
    if len(broker_nfo) > 2:
        etcd_port = broker_nfo[2]
    # print(f"host: {host}")
    # print(f"beanstalk_port: {beanstalk_port}")
    # print(f"etcd_port: {etcd_port}")
    return SimpleNamespace(host=host, beanstalk_port=beanstalk_port, etcd_port=etcd_port)
