from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from elasticsearch import helpers
from elasticsearch_dsl import Search
# from elasticsearch_dsl import connections

from confluent_kafka import Producer
from confluent_kafka import Consumer
from numpyencoder import NumpyEncoder

import json
import pandas as pd
import tensorflow as tf
from tensorflow.keras import datasets, layers, models
import numpy as np

import socket
import time
import gc
import datetime
import sys
import os 


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

def initial_model_parameters (dataset_name):
    """This function initializes the model parameters for the given dataset such as CIFAR-10 and MNIST datasets

    Args:
        dataset_name (str): for now two types of datasets are supported CIFAR-10 and MNIST

    Returns:
        weights: numpy array
        model: Tensorflow model
        
    """
    if dataset_name == 'CIFAR-10':
        model = models.Sequential()
        model.add(layers.Conv2D(16, (3, 3), activation='relu', input_shape=(32, 32, 3)))
        model.add(layers.MaxPooling2D((2, 2)))
        model.add(layers.Conv2D(32, (3, 3), activation='relu'))
        model.add(layers.MaxPooling2D((2, 2)))
        model.add(layers.Conv2D(64, (3, 3), activation='relu'))
        model.add(layers.Flatten())
        model.add(layers.Dense(64, activation='relu'))
        model.add(layers.Dense(10))
        model.summary()
        model.compile(tf.keras.optimizers.Adam(0.001),
                    loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                    metrics=['accuracy'])
        weights = model.get_weights()
        return weights ,model
    
    elif dataset_name == "MNIST":
        model = tf.keras.models.Sequential([
            tf.keras.layers.Flatten(input_shape=(28, 28)),
            tf.keras.layers.Dense(32, activation='relu'),
            tf.keras.layers.Dense(10)
            ])
        model.compile(
            optimizer=tf.keras.optimizers.Adam(0.001),
            loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
            metrics=['accuracy'],
            )
        weights = model.get_weights()
        return weights, model
    else:
        print("Unknown dataset!")

def eval_model(model,weights,train_images,train_labels,test_images,test_labels):
    """ This function evaluates the model performance against the training and testing images for given dataset.

    Args:
        model (tensorflow model): tensorflow model
        weights (np.array): weights of the model
        train_images (np.array): training images 
        train_labels (np.array): training labels
        test_images (np.array): testing images
        test_labels (np.array): testing labels

    Returns:
        results_train: training accuracy
        results_test: testing accuracy
    """
    tf.keras.backend.clear_session()
    model.set_weights(weights)
    results_test = model.evaluate(test_images, test_labels)
    results_train = model.evaluate(train_images, train_labels)
    
    gc.collect()
    return results_train[1],results_test[1]


def acked(err, msg):
    """ This function is used to check if the Kafka Broker delivers the messages correctly. 

    Args:
        err (err): error message
        msg (str): kafka message
    """
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))



def json_serializer(data):
    """This function is used to serialize the messages in the Kafka Broker

    Args:
        data (json): json message to be serialized and sent through the Kafka Broker

    Returns:
        message: serialized message object
    """
    return json.dumps(data,cls=NumpyEncoder).encode('utf-8')

def json_datetime_seralizer(data):
    """ This function is used to serialize the messages in the Kafka Broker when it has datetime object in it.

    Args:
        data (json): json message to be serialized and sent through the Kafka Broker

    Returns:
        message: serialized message object
    """
    return json.dumps(data,default=str)


def es_delete_index(es,indices):
    """ This function deletes an index from elasticsearch database.

    Args:
        es (Elasticsearch): elasticsearch databased connection object.
        indices (list of strings): The list of indices to delete.

    Raises:
        exnodes: if the index is not found in elasticsearch database it will raise an exception
    """
    try:
        es.delete_by_query(index=indices, body={"query": {"match_all": {}}})
    except RequestError as ex:
        if ex.error == 'No index found!':
            pass # Index already exists. Ignore.
        else: # Other exception - raise it
            raise exnodes

def es_get_indices(es):
    """ This function gets all the indices in the database.

    Args:
        es (Elasticsearch): Elasticsearch connection object.

    Returns:
        list: list of the indices on the database.
    """
    return es.indices.get_alias("*")


def check_client_reported_parameters(es,iter_num):
    """ This function checks the number of messages sent by the users for specific iteration number.

    Args:
        es (Elasticsearch): Elasticsearch connection object.
        iter_num (int): training iteration number

    Returns:
        int: the number of messages sent by the users for specific iteration number
    """
    s = Search(using=es,index="client-message",doc_type='doc')
    s = s.query("match",iter_num=iter_num)
    s = s.source(excludes=["parameters","test_accuracy","train_accuracy","time","dataset"])
    response=s.execute(ignore_cache=True)
    return response['hits']['total']['value']



def federated_averging_elastic(iter_num,client):
    """ This function calculates the Federated Averaging algorithm for specific iteration number using the data on elasticsearch database.

    Args:
        iter_num (int): iteration number
        client (Elasticsearch): The elasticsearch client
    Returns:
        average (np.array): the average of the all client parameters using Federated Averaging Algorithm.
    """
    
    
    if client.ping():
        print("The connection was successful!")
        resp = client.info()
#         print(json.dumps(resp,indent=4, sort_keys=True))
    else:
        print("Connection Error!")
        
    s = Search(using=client,index="client-message",doc_type='doc')
    s = s.query("match",iter_num=iter_num)
    print(s.to_dict())
    response = s.execute(ignore_cache=True)
    if response.success():
        print("Query Succeded!")
        df=pd.DataFrame((d.to_dict() for d in s.params(size=100).scan()))
        df = df.dropna()
#         print(df.head)
    else:
        print("Warning")
    all_data= df['parameters'].to_list()
    shape = np.shape(all_data)
    average = np.zeros_like(df['parameters'][0])
    for i in range(shape[1]):
        temp=[]
        for j in range(shape[0]):
            temp.append(all_data[j][i])
        average[i]=np.array(np.mean(temp,axis=0),dtype=np.float32)
    return average


def pull_query(iter_num,client):
    """ This function calls the federated averaging function for specifc iteration number using the given elasticsearch client.

    Args:
        iter_num (int): iteration number
        client (Elasticsearch): Elasticsearch client

    Returns:
        federated_average (np.array): Federated Average
        iter_num (int): iteration number
    """
    federated_average = federated_averging_elastic(iter_num=iter_num,client=client)
    return federated_average,iter_num
 
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

def get_latest_users(client,query_index):
    """ This function gets the latest number of users for specific dataset.

    Args:
        client (Elasticsearch): Elasticsearch client.
        query_index (str): the index to find the number of users.

    Returns:
        latest_users (int): number of users for the specified dataset.
        dataset_name (str): name of the dataset
    """
    s = Search(using=client,index=query_index,doc_type='doc')
    s.sort('des','iter_num')
    response=s.execute()
    if response.success():
        latest_users = response[-1]['num_user']
        dataset_name = response[-1]['service']
        goal = response[-1]['goal']
        if latest_users== None:
            return 0 , None
        else:
            return latest_users, dataset_name , goal
    else:
        print("Error: Couldn't run the query.")    


def get_users(es):
    """ This function will return a list of users registered to the Micro-FL.

    Args:
        es (Elasticsearch): elasticsearch client

    Returns:
        users (list): list of users registered to Micro-FL.
    """
    s = Search(using=es,index="users",doc_type='doc')
    response= [x for x in s.scan()]
    users = [x["username"] for x in response]
    return users

def get_users_ready_iteration (es,iteration_num):
    """ gets the number of users ready for the next training iteration.

    Args:
        es (Elasticsearch): Elasticsearch client.
        iteration_num (int): iteration number.

    Returns:
        list_users: list of users ready for next training iteration.
    """
    s = Search(using=es,index="topic-user-status",doc_type='doc')
    s = s.query('term',iter_num=iteration_num)
    response = [x for x in s.scan()]
    list_users = []
    for x in response:
        list_users.append(x["username"])
    list_users = np.unique(list_users)
    return list_users

def wait_control(consumer):
    """ This function will start consuming data from "topic-control" when the message is 
    received it will start the experiment.

    Args:
        consumer (Kafka.Consumer): kafka consumer

    Raises:
        KafkaException: if the message has any error. It will rase a message error object.

    Returns:
        dataset_name (str): dataset name
        num_users (int): number of users
    """
    try:
        consumer.subscribe(["topic-control"])
        print("Waiting for the control signal!")
        while True:
            time.sleep(5)
            print("Waiting for the control signal!")
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                    (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error()) 

            else:
                print("Messege Received!")
                message = msg.value().decode('utf-8')
                message=json.loads(message)
                dataset_name = message['dataset']
                num_users = message['num_users']
                goal = message['goal']
                break
        consumer.close()
        return dataset_name, num_users , goal
    except:
        print("exception happened in control message. Closing the conumser!")
        consumer.close()



def aggregator_main(producer,client,model,initial_weights,num_users,dataset_name='CIFAR-10',goal=2000):
    """ This function run the main aggregator function.

    Args:
        producer (Kafka.Producer): kafka producer
        client (Elasticsearch): Elasticsearch client
        model (Tensorflow.model): tensorflow model
        initial_weights (np.array): initialized model parameters
        num_users (int): number of users in the Micro-FL.
        dataset_name (str, optional): Dataset name. Defaults to 'CIFAR-10'.
        goal (int, optional): number of iteration to be done in federated scenario. Defaults to 2000.
    """
    user_check = True

    while user_check:
        print("Checking the number of available Users")
        if len(get_users(client))!= num_users or len(get_users_ready_iteration(client,0))!=num_users:
            time.sleep(10)
            print(len(get_users(client)))
        else: 
            user_check = False

    ## Get the dataset 
    if dataset_name == 'CIFAR-10':
        (train_images, train_labels), (test_images, test_labels) = datasets.cifar10.load_data()
    elif dataset_name == 'MNIST':
        (train_images, train_labels), (test_images, test_labels) = datasets.mnist.load_data()

    train_images = train_images/255.0
    test_images = test_images/255.0

    # i = 0
    service = dataset_name
    iter_num = get_latest_iter_num(client,"aggregator-message")

    if iter_num == 0 :
        train_ac,test_ac = eval_model(model = model, weights=initial_weights,train_images=train_images,train_labels=train_labels,test_images=test_images,test_labels=test_labels)

        message = {
            'iter_num':iter_num,
            'goal' : goal,
            'service':service,
            'num_user':num_users,
            'train_accuracy':train_ac,
            'test_accuracy' : test_ac,
            'exec_time':0,
            'time':json_datetime_seralizer(datetime.datetime.now()),
            'parameters':initial_weights
            }
        

        producer.produce(topic,value=json_serializer(message),callback=acked)
        producer.flush()
        federated_avg= initial_weights
        print("Parameters Initialized")
        iter_num = iter_num+1
        # time.sleep(120)
    else:
        iter_num= iter_num+1
        
        
        
    while iter_num < goal :
        current_num_users_ready = len(get_users_ready_iteration(client,iter_num))

        if current_num_users_ready == num_users:
            all_users = len(get_users(client))
            while True:
                try:
                    ready_users = check_client_reported_parameters(client,iter_num)
                    if ready_users==all_users:
                        break
                except:
                    print("exception in number of users or database connection!")
                    
            time_start = time.time()
            federated_avg, iter_num= pull_query(iter_num=iter_num,client=client)
            time_end = time.time()
            
            train_ac,test_ac = eval_model(model = model, weights=federated_avg,train_images=train_images,train_labels=train_labels,test_images=test_images,test_labels=test_labels)

            message = {
                'iter_num':iter_num,
                'goal' : goal,
                'service':service,
                'num_user':num_users,
                'train_accuracy':train_ac,
                'test_accuracy' : test_ac,
                'exec_time':time_end-time_start,
                'time':json_datetime_seralizer(datetime.datetime.now()),
                'parameters':federated_avg
                }

            producer.produce(topic,value=json_serializer(message),callback=acked)
            producer.flush()
            print("Message {} is sent successfuly.".format(iter_num))
            # i=i+1
            iter_num = iter_num+1
        else:
            time.sleep(10)


#################3 The main Code starts from here 

if __name__=="__main__":
    """ This is the main program. 
    """
    
    
    # getting the environment variables from the command line for Elasticsearch Password
    ELASTIC_PASSWORD = os.environ['ELASTIC_PASSWORD']
    bootstrap_server= "my-cluster-kafka-bootstrap:9092"
    ##### Connecting to the Elastic Search Database
    
    es = Elasticsearch(
    ['mikael-elasticsearch-es-internal-http'],
    http_auth=('elastic', f'{ELASTIC_PASSWORD}'),
    scheme="http",
    port=9200,
    retry_on_timeout= True,
    timeout=300,
        )
    
    # Checking if Elasticsearch is running!
    if es.ping():
        print("The connection was successful!")
        resp = es.info()
        print(json.dumps(resp,indent=4, sort_keys=True))
    else:
        print("Connection Error!")

    #### Creating the Indexes
    es_create_index_if_not_exists(es, "client-message")
    es_create_index_if_not_exists(es, "aggregator-message")
    

    # KAFKA initalziation

    # bootstrap_server= "broker:9092,broker2:9093,broker3:9094"
    print("The host name is :",socket.gethostname())

    conf_cons = {'bootstrap.servers': bootstrap_server,
            'group.id': "Aggregator",
            'enable.auto.commit': False,
            'auto.offset.reset': 'latest',
            'retries':3,'retry.backoff.ms':5000,'linger.ms': 250,}
    consumer = Consumer(conf_cons)
    # topics = ["topic_control"]
    conf_pro = {'bootstrap.servers':bootstrap_server,"message.max.bytes": 1000000000,
            'client.id': socket.gethostname(),"compression.type":'gzip','retries':3,'retry.backoff.ms':5000,'linger.ms': 250,'acks':'all','enable.idempotence':True}
    producer = Producer(conf_pro)
    topic = 'aggregator-message'

    ### Download the dataset
    if  get_latest_iter_num(es,"aggregator-message") == 0:
        dataset_name,num_users,goal =  wait_control(consumer)
        print(dataset_name, num_users)
    else:
        # iter_num = get_latest_iter_num(es,"aggregator_message")
        num_users,dataset_name,goal= get_latest_users(es,"aggregator-message")
        
        
    # dataset_name = 'CIFAR-10'
    ###### Here is where we run the federated Code
    weights,model = initial_model_parameters(dataset_name=dataset_name)
    aggregator_main(producer=producer,client=es,model=model,initial_weights=weights,num_users =num_users,dataset_name=dataset_name,goal=goal)



