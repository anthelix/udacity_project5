
#!/bin/python3

import json
import subprocess
import yaml


secrets = yaml.full_load(open('settings/secret.yaml'))['airflow']

#json_variables = json.loads(secrets['variables'])
#with open('/tmp/variables.json', 'a') as tmp_file:
#    json.dump(json_variables, tmp_file, sort_keys=True, indent=4)
#
#subprocess.Popen("/entrypoint.sh airflow variables -i /tmp/variables.json; rm -f /tmp/variables.json",
#                 shell=True).wait()

for conn in secrets['connections']:
    params = f"--conn_id {conn['id']} --conn_type {conn['type']} "
    if conn.get('uri'):
        params += f"--conn_uri {conn['uri']} "

    if conn.get('host'):
        params += f"--conn_host {conn['host']} "

    if conn.get('login'):
        params += f"--conn_login {conn['login']} "

    if conn.get('password'):
        params += f"--conn_password {conn['password']} "

    if conn.get('schema'):
        params += f"--conn_schema {conn['schema']} "

    if conn.get('port'):
        params += f"--conn_port {conn['port']} "

    if conn.get('extra'):
        params += f"--conn_extra {conn['extra']}"

    subprocess.Popen(f"/entrypoint.sh airflow connections -a {params}", shell=True).wait()