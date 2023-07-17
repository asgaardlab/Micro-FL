import elasticsearch
import json
from elasticsearch import Elasticsearch , RequestError
from elasticsearch_dsl import Search
# from confluent_kafka.admin import AdminClient, NewTopic
# from confluent_kafka import KafkaException
from numpyencoder import NumpyEncoder
from tensorflow.keras import datasets
import numpy as np
import os

def init_database():
    ELASTIC_PASSWORD = os.environ['ELASTIC_PASSWORD']
    # es = Elasticsearch([
    #     {'host': '34.170.8.222'},
    #     { 'port': 9200, 'url_prefix': 'es', 'use_ssl': True}],timeout=300,http_compress = True,max_retries=1, retry_on_timeout=True)
    es = Elasticsearch(
    ['mikael-elasticsearch-es-internal-http'],
    http_auth=('elastic', f'{ELASTIC_PASSWORD}'),
    scheme="http",
    port=9200,
        )
    return es

def json_serializer(data):
    return json.dumps(data,cls=NumpyEncoder).encode('utf-8')

def json_datetime_seralizer(data):
    return json.dumps(data,default=str)


def es_create_index_if_not_exists(es, index):
    """Create the given ElasticSearch index and ignore error if it already exists
    Args:
        es : Elasticsearch Server
        index : Elasticsearch index
    Returns: None
    """
    body = {
        'settings': {
            'index': {
                "number_of_shards": 5,  
                "number_of_replicas": 3
                    }
                    }}
    try:
        es.indices.create(index,body)
    except RequestError as ex:
        if ex.error == 'resource_already_exists_exception':
            pass # Index already exists. Ignore.
        else: # Other exception - raise it
            raise ex

def check_connection(elastic_server):
    if elastic_server.ping():
        print("The connection was successful!")
        resp = elastic_server.info()
        print(json.dumps(resp,indent=4, sort_keys=True))
    else:
        print("Connection Error!")

def UserExist(username):
    s = Search(using=es,index="users",doc_type='doc')
    # s = s.filter("term",username=username)s
    s = s.query('term',username__keyword=username)
    response= [x for x in s.scan()]
    if len(response)==0:
        return False
    else:
        return True

def es_delete_index(es,indices):
    try:
        es.delete_by_query(index=indices, body={"query": {"match_all": {}}})
    except RequestError as ex:
        if ex.error == 'No index found!':
            pass # Index already exists. Ignore.
        else: # Other exception - raise it
            raise exnodes
            
def get_users(es):
    s = Search(using=es,index="users",doc_type='doc')
    response= [x for x in s.scan()]
    users = [x["username"] for x in response]
    try: 
        if users != []:
            return users
    except:
        print("User list is empty!")

def prepare_dataset(users,dataset_name=None):
    if dataset_name == "CIFAR-10":
        (train_images, train_labels), (test_images, test_labels) = datasets.cifar10.load_data()
    elif dataset_name == "MNIST":
        (train_images, train_labels), (test_images, test_labels) = datasets.mnist.load_data()
    
    user_dataset = dict.fromkeys(users)
    num_users = len(users)
    train_sample_num = np.shape(train_images)[0]
    test_sample_num  = np.shape(test_images)[0]
    rand_train_index = np.random.choice(np.arange(train_sample_num),size=(num_users,train_sample_num//num_users))
    rand_test_index = np.random.choice(np.arange(test_sample_num),size=(num_users,test_sample_num//num_users))
    for i in range(num_users):
        user_dataset[users[i]] = (rand_train_index[i],rand_test_index[i])
    return user_dataset

def publish_index(es,dataset=None):
    for username in dataset.keys():
        message = {'username':username,'train_index':np.array(dataset[username][0]),'test_index':np.array(dataset[username][1])}
        es.index(index="datasets",id=message['username'],document=message)
        

def delete_userdata(es):
    try:
        es_delete_index(es,"datasets")
        es_delete_index(es,"users")
        es_delete_index(es,"topic-user-status")
        es_delete_index(es,"client-message")
        es_delete_index(es,"aggregator-message")
    except:
        print("exception in delete_userdata")
    print("Deleted indexs for dataset and usernames!")
    
    
def make_dataset(es,dataset_name="CIFAR-10"):
    users = get_users(es)
    print(f"users found in Micro-FL: {users}")
    dataset = prepare_dataset(users,dataset_name=dataset_name)
    publish_index(es,dataset=dataset)
    print("Successfully created users!")

    
