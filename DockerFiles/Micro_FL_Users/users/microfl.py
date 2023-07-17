import datetime
from numpyencoder import NumpyEncoder
import socket
import requests
import numpy as np
import json
from confluent_kafka import Producer
from confluent_kafka import Consumer
import tensorflow as tf
from tensorflow.keras import datasets, layers, models
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
import gc 
import multiprocessing as mp
from random_username.generate import generate_username
import time
import os 

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))
def json_deserialize(data):
    return json.loads(data.json())

def json_serializer(data):
    return json.dumps(data,cls=NumpyEncoder).encode('utf-8')

def json_datetime_seralizer(data):
    return json.dumps(data,default=str)


class User(mp.Process):

    def __init__ (self,username,password,service):
        # must call this before anything else
        mp.Process.__init__(self)
        gc.enable()
        self.IP_CONF = os.environ['IP_CONF']
        self.IP_WEB = os.environ['IP_WEB']
        self.REG_ENDPOINT = f'http://{self.IP_WEB}:80/register'
        self.LOG_ENDPOINT = f'http://{self.IP_WEB}:80/login'
        self.INDEX_ENDPOINT = f'http://{self.IP_WEB}:80/get_index'
        self.bootstrap_server = f"{self.IP_CONF}:9094"
        self.TOPIC_CLIENT = "client-message"
        self.TOPIC_AGG = ["aggregator-message"]
        self.TOPIC_STATUS = "topic-user-status"
        self.username = username
        self.password = password
        self.service = service
        self.user_register()
        self.dataset= None
        print(f"User {self.username} is created! ")

    def user_register(self):
        registerMessage = {'username':self.username,'password':self.password}
        headers={'Content-Type' : 'application/json'}
        response = requests.post(url=self.REG_ENDPOINT,headers=headers,data=json.dumps(registerMessage))
        return json.loads(response.text)
    
    def user_login(self):
        loginMessage = {'username':self.username,'password':self.password,'MS':self.service}
        headers={'Content-Type' : 'application/json'}
        response = requests.post(url=self.LOG_ENDPOINT,headers=headers,data=json.dumps(loginMessage))
        return json.loads(response.text)
    
    def send_user_status(self,status,iter_num):
        statusTopic = self.TOPIC_STATUS
        statusMessage = {'username':self.username, 'last_access':json_datetime_seralizer(datetime.datetime.now()),'status':status,'iter_num':iter_num}
        self.producer.produce(statusTopic, value=json_serializer(statusMessage),callback=acked)
        self.producer.flush()


    def get_dataset_index(self):
        requestMessage = {'username':self.username,'password':self.password,'MS':self.service}
        headers={'Content-Type' : 'application/json'}
        response = requests.post(url=self.INDEX_ENDPOINT,headers=headers,data=json.dumps(requestMessage))
        json_response = json_deserialize(response)
        return json_response["train_index"],json_response["test_index"]
    
    def set_dataset(self,user_dataset):
        self.dataset = user_dataset
        gc.collect()
        # return self.user_dataset
    
    def init_model(self):

        if self.service == 'CIFAR-10':
            model = models.Sequential()
            model.add(layers.Conv2D(16, (3, 3), activation='relu', input_shape=(32, 32, 3)))
            model.add(layers.MaxPooling2D((2, 2)))
            model.add(layers.Conv2D(32, (3, 3), activation='relu'))
            model.add(layers.MaxPooling2D((2, 2)))
            model.add(layers.Conv2D(64, (3, 3), activation='relu'))
            model.add(layers.Flatten())
            model.add(layers.Dense(64, activation='relu'))
            model.add(layers.Dense(10))
            # model.name=self.username
            model.compile(
                optimizer=tf.keras.optimizers.Adam(0.001),
                loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
                metrics=['accuracy']
                )
            return model
        if self.service == 'MNIST':
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
            return model

    def train_model(self, weights,train_images,train_labels,test_images,test_labels):
        tf.keras.backend.clear_session()
        array_weights= []
        for i in range(len(weights)):
            array_weights.append(np.asarray(weights[i]))

        self.model.set_weights(array_weights)
        del array_weights
        gc.collect()

        if self.service == 'CIFAR-10':
            history = self.model.fit(train_images, train_labels,verbose=0, epochs=5, batch_size =10,
                            validation_data=(test_images, test_labels))
        elif self.service == 'MNIST':
            history=self.model.fit(train_images, train_labels,verbose=0, epochs=5, batch_size =10,
                        validation_data=(test_images, test_labels))
        new_weights = self.model.get_weights()
            
        gc.collect()

        return new_weights ,[history.history['accuracy'][-1],history.history['val_accuracy'][-1]]
    
    
    
    def run(self):
        
        ''' Because of the KAFKA problems with child process and forking 
            I had to create the producer and the consumer in the child process
            this def run() is where the multiprocessing is happening.
        '''
        self.bootstrap_server = f"{self.IP_CONF}:9094"
        self.producerConfig = {'bootstrap.servers':self.bootstrap_server,'client.id': socket.gethostname(),"message.max.bytes": 1000000000,'retries':3,'retry.backoff.ms':5000,'linger.ms': 250,'acks':'all','enable.idempotence':True,"compression.type":'gzip'}
        self.consumerConfig = {'bootstrap.servers':self.bootstrap_server,'group.id': self.username,'enable.auto.commit': False, 'auto.offset.reset': 'latest','retries':3,'retry.backoff.ms':5000,'linger.ms': 250 }
        self.producer = Producer(self.producerConfig)
        self.consumer = Consumer(self.consumerConfig)
        self.consumer.subscribe(self.TOPIC_AGG) 
        self.model = self.init_model()
        self.model.summary()
        (train_images, train_labels), (test_images, test_labels) = self.dataset
        del self.dataset
        gc.collect()
        try:
            self.send_user_status(status = "Init",iter_num=0)
            while True:
                time.sleep(5)
                msg = self.consumer.poll(timeout=1.0)
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
                    weights = message['parameters']
                    iter_num = message['iter_num']

                    time_start = time.time()
                    user_weights, accuracy = self.train_model(weights,train_images,train_labels,test_images,test_labels)
                    time_end = time.time()
                    new_message = {
                        'username':self.username,
                        "dataset":self.service,
                        'iter_num':iter_num+1,
                        "train_accuracy":accuracy[0],
                        "test_accuracy":accuracy[1],
                        "parameters":user_weights,
                        "time": json_datetime_seralizer(datetime.datetime.now()),
                        'exec_time':time_end-time_start
                        }
                    self.producer.produce("client-message", value=json_serializer(new_message),callback=acked)
                    self.send_user_status(status = "Ready",iter_num=iter_num+1)
                    # print("Message sent Successfully")
                    del(message,weights,new_message,user_weights,accuracy,msg)
                    gc.collect()

        except:
            print("exception happened.")
        finally:
            self.consumer.close()
            print(f"consumer {self.username} is closed.")






class Swarm :
    def __init__ (self,num_users,dataset=None):
        self.num_users = num_users
        self.dataset_name = dataset
        self.dataset = self.get_whole_dataset()
        self.usernames = self.create_usernames()
        self.users = self.generate_users()
        self.swarm_name = "Swarm-"+generate_username(1)[0]
        self.TOPIC_CONTROL = ["topic_control"]
        self.IP_CONF = os.environ['IP_CONF']
        self.IP_WEB = os.environ['IP_WEB']
        self.bootstrap_server = f"{self.IP_CONF}:9094"
        self.swarm_producerConfig = {'bootstrap.servers':self.bootstrap_server,'client.id': self.swarm_name,"message.max.bytes": 1000000000,'retries':3,'retry.backoff.ms':5000,'linger.ms': 250,'acks':'all','enable.idempotence':True,"compression.type":'gzip'}
        self.swarm_consumerConfig = {'bootstrap.servers':self.bootstrap_server,'group.id': self.swarm_name,'enable.auto.commit': False, 'auto.offset.reset': 'latest','retries':3,'retry.backoff.ms':5000,'linger.ms': 250}
        self.swarm_producer = Producer(self.swarm_producerConfig)
        self.swarm_consumer = Consumer(self.swarm_consumerConfig)
        self.swarm_consumer.subscribe(self.TOPIC_CONTROL)         
    def create_usernames(self):
        users = []
        for i in range(self.num_users):
            one_user = generate_username(1)[0]+generate_username(1)[0]
            users.append(one_user)
        
        # usernames = []
        # for i in range(self.num_users):
        #     usernames.append(f"user_{i}")
        return users
    
    def generate_users(self):
        users = []
        for i in range(self.num_users):
            users.append(User(username= self.usernames[i],password=self.usernames[i],service=self.dataset_name))
        return users
    
    def wait_control(self):
        try:
            while True:
                time.sleep(5)
                print("Waiting for the control signal!")
                msg = self.swarm_consumer.poll(timeout=1.0)
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
                    break
        except:
            print("exception happened.")
        finally:
            self.swarm_consumer.close()
            print(f"consumer {self.swarm_name} is closed.")



    def get_whole_dataset(self):
        if self.dataset_name=="CIFAR-10":
            (train_images, train_labels), (test_images, test_labels) = datasets.cifar10.load_data()
        elif self.dataset_name=="MNIST" :
            (train_images, train_labels), (test_images, test_labels) = datasets.mnist.load_data()
        return (train_images, train_labels), (test_images, test_labels)
    
    def set_user_datasets(self):
        for user in self.users:
            train_index, test_index = user.get_dataset_index()
            selected_train_images, selected_test_images = self.dataset[0][0][train_index] / 255.0, self.dataset[1][0][test_index] / 255.0
            selected_train_labels, selected_test_labels = self.dataset[0][1][train_index] , self.dataset[1][1][test_index]
            user.set_dataset([(selected_train_images, selected_train_labels), (selected_test_images, selected_test_labels)])        
    

    def run_fed(self):
        try:
            for user in self.users:
                    print(f"Starting user : {user.username} process.")
                    user.start()
            for user in self.users:
                    print(f"Joining user: {user.username}.")
        except:
            print("Exception in the process start/join!")

            
            
    def describe(self):
        print(f"The number of users generated is : {self.num_users}")
        print(f"The generated usernames are : {self.usernames}")
        print(f"Downloaded dataset is {self.dataset_name}")
    
    def __len__ (self):
        return len(self.users)
    
    
    
