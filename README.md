# fwq


## CLI

```



fwq_job = {
    type: {job_type},                              # required 
    broker_id: {broker_id},                        # required 
    app: {app_name},                               # required 

    name: {job_name}, 
    priority: {priority}, 
    delay_secs: {delay_secs}, 
    ttr: {ttr}, 
    data: { {job_data_dict} }, 

    state: {state}, 
    job_id: {job_id}, 
    activity: [ {activity_row_list} ]
}
fwq_job_brief = {
    type: {job_type},  
    name: {job_name}, 
    broker_id: {broker_id},
    app: {app_name}, 

    state: {state}, 
    job_id: {job_id}, 
}

{broker_id}/{app}/jobs/{state}/{job_id}: {timestamp}

{broker_id}/{app}/{job_id}/type: {type}
{broker_id}/{app}/{job_id}/name: {name}
{broker_id}/{app}/{job_id}/data: {data}
{broker_id}/{app}/{job_id}/state: {state}
{broker_id}/{app}/{job_id}/activity/{timestamp}: {message}

broker_id = f"{broker_host_name_or_ip}_{beanstalkd_port}_{etcd_client_port}"

fwq config set --spec, --id --value
fwq config get
fwq config reset

fwq broker start                        # outputs the broker_id  
fwq broker stop
fwq broker add --id
fwq broker rm --id

fwq job nq --spec       
fwq job get --id                  # outputs an fwq_job
fwq job list --id --states             # outputs list of fwq_job_briefs  

fwq job do_next --id --wait
fwq job do_all --id --wait

fwq job kick --id --count --wait
fwq job purge --app --count --state

fwq worker retire --id

```

Use cases:

 - Run a script as a background job
 - Get info (state/status, activity, specs) about a specific job
 - Get a list of jobs
 - Purge completed jobs older than <number_seconds>
 - Purge a specific job
 - Stop a running job
 - Kick jobs

Notes:


