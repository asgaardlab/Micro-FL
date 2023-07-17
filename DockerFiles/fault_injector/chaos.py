# nohup python fault.py > chaos_logs.log 2>&1 &
# curl http://mikael-elasticsearch-es-internal-http.kafka.svc.cluster.local:9200
# curl -u "elastic:elasticpassword" -k "http://mikael-elasticsearch-es-http.default.svc.cluster.local:9200"

# http://mikael-elasticsearch-es-http.kafka.svc.cluster.local
import json
import os
import yaml
import subprocess
import time
from elasticsearch_dsl import Search
from elasticsearch import Elasticsearch


def get_latest_iter_num(client,query_index):
    """ This function gets the latest iteration number from the elastic database.

    Args:
        client (Elasticsearch): Elasticsearch client.
        query_index (str): the iteration number index.

    Returns:
        iter_num (int): iteration number
    """
    s = Search(using=client,index=query_index,doc_type='doc')
    s.aggs.metric('max_iteration', 'max', field='iter_num')
    response=s.execute()
    if response.success():
        iter_num = response.aggregations.max_iteration.value
        if iter_num== None:
            return 0
        else:
            return iter_num
    else:
        print("Error: Couldn't run the query.")   


def create_fault_yaml (filename='fault.yaml' , exp_name="kafka" ,duration='5m' ):
    exp_config = {
     'kind': 'PodChaos',
     'apiVersion': 'chaos-mesh.org/v1alpha1',
     'metadata': {'namespace': 'kafka', 'name': 'kafka-1'},
     'spec': {'selector': {'namespaces': ['kafka'],
       'labelSelectors': {'app.kubernetes.io/name': 'kafka'}},
      'mode': 'one',
      'action': 'pod-failure',
      'duration': '5m'}}
    exp_config['metadata']['name'] = exp_name
    exp_config['spec']['duration'] = duration
    json_obj = json.dumps(exp_config)
    print(json_obj)
    cwd = os.getcwd()
    with open(f"{cwd}/{filename}", 'w') as f:
        f.write(json_obj)
    print(f"Yaml file created for {exp_name}.")

def inject_fault(filename):
    # Define the kubectl command to execute
    kubectl_command = f"kubectl apply -f {filename}"

    # Execute the kubectl command in a subprocess and capture the output
    output = subprocess.check_output(kubectl_command.split())

    # Decode the output from bytes to a string
    output_str = output.decode('utf-8')

    # Print the output
    
    print("fault injected")



if __name__=="__main__":
    # Initialize elasticsearch connection
    ELASTIC_PASSWORD = 'elasticpassword'
    es = Elasticsearch(
    ['mikael-elasticsearch-es-internal-http'],
    http_auth=('elastic', f'{ELASTIC_PASSWORD}'),
    scheme="http",
    port=9200,
    retry_on_timeout= True,
    timeout=300,
        )
    # es_namespace = "kafka"
    # es.transport.request("PUT", f"/_cluster/settings", body={"transient": {"cluster.routing.allocation.include._namespace": es_namespace}})

        # Checking if Elasticsearch is running!
    if es.ping():
        print("The connection was successful!")
        resp = es.info()
        print(json.dumps(resp,indent=4, sort_keys=True))
    else:
        print("Connection Error!")
        
        
    # Define the iteration numbers for fault injection    
    fault_iter_num = [20,40,60,80]
    
    for i in fault_iter_num:
        # Get the latest iteration number
        while True:
            try:
                iter_num = get_latest_iter_num(client=es,query_index='aggregator-message')
                print(f"Latest iteration number: {iter_num}")
                
                if iter_num == 0 or iter_num == None:
                    print("No iteration number found.")
                    continue
            except :
                print("Failed to get latest iteration number")
            
            if iter_num == i:
                print("Getting Ready for Fault Injection")
                # Create the fault.yaml file
                create_fault_yaml(exp_name=f"kafka-{i}",duration='5m')
                # Inject the fault
                inject_fault(filename='fault.yaml')
                time.sleep(300)
                break
            else:
                # Wait for 1 minute
                time.sleep(30)
    print("Fault Injection Done")
    
