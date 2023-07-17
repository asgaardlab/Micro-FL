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
import sys

def acked(err, msg):
    """ Message Acknowledgement function

    Args:
        err (Kafka Error): Error returned from message broker
        msg (Kafka Message): message sent by message broker
    """
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def json_deserialize(data):
    """ Deserialize the data obtianed from the webserver.

    Args:
        data (Json Object): Data obtained from 

    Returns:
        numpy.array : numpy.array object will be returned
    """
    return json.loads(data.json())

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


class User(mp.Process):
    """This class create a process fro a user that contributes to the federated learning.

    Args:
        mp (multiprocess): multiprocess module.
    """
    def __init__ (self,username,password,service):
        """ This function initalizes a user with a username and password and its subscribed service.

        Args:
            username (string): username to be used
            password (string): password to be used
            service (string): subscribed service
        """
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
        """Thi function is called when we need to register a user to the Micro-FL platform.

        Returns:
            respnose.text: response from the server
        """
        registerMessage = {'username':self.username,'password':self.password}
        headers={'Content-Type' : 'application/json'}
        response = requests.post(url=self.REG_ENDPOINT,headers=headers,data=json.dumps(registerMessage))
        return json.loads(response.text)
    
    def user_login(self):
        """ This function checks if the user can login to the server.

        Returns:
            response (string): response of the server.
        """
        loginMessage = {'username':self.username,'password':self.password,'MS':self.service}
        headers={'Content-Type' : 'application/json'}
        response = requests.post(url=self.LOG_ENDPOINT,headers=headers,data=json.dumps(loginMessage))
        return json.loads(response.text)
    
    def send_user_status(self,status,iter_num):
        """ This function sends the user status when the user is ready to contribute to the Micro-FL.

        Args:
            status (string): status of the user, as a string. e.g. "Ready"
            iter_num (int): iteration number of the user.
        """
        statusTopic = self.TOPIC_STATUS
        statusMessage = {'username':self.username, 'last_access':json_datetime_seralizer(datetime.datetime.now()),'status':status,'iter_num':iter_num}
        self.producer.produce(statusTopic, value=json_serializer(statusMessage),callback=acked)
        self.producer.flush()

    def get_dataset_index(self):
        """ This function will get the indices of the dataset that user needs to train the model on.
        This function is defined for simulation purposes.

        Returns:
            train_index(numpy.array) , test_index(numpy.array) : training and testing indices from the web application.
        """
        requestMessage = {'username':self.username,'password':self.password,'MS':self.service}
        headers={'Content-Type' : 'application/json'}
        response = requests.post(url=self.INDEX_ENDPOINT,headers=headers,data=json.dumps(requestMessage))
        json_response = json_deserialize(response)
        return json_response["train_index"],json_response["test_index"]
    
    def set_dataset(self,user_dataset):
        """This function will set the indices of the dataset that user needs to train the model.
        We need to call the get_dataset_index before this function.

        Args:
            user_dataset (None): Nothing is returned.
        """
        self.dataset = user_dataset
        gc.collect()
    
    def init_model(self):
        """This function initializes the model for a specific service.

        Returns:
            model (Tensorflow Model): _description_
        """
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
        """This function train the model using a given set of training and testing datasets.

        Args:
            weights (numpy.array): weights obtained from federated training.
            train_images (numpy.array): training dataset.
            train_labels (numpy.array): training labels dataset.
            test_images (numpy.array): testing dataset.
            test_labels (numpy.array): testing labels.

        Returns:
            new_weights (numpy.array): new trained model parameters.
            training accuracy (float): training accuracy.
            testing accuracy (float): testing accuracy.
        """
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
        """This function runs as a user which is training the model in the federated training environment.

        Raises:
            KafkaException: closes the consumer if something goes wrong.
        """
        
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
    """This class is used to create a set of users from user class.
    """
    def __init__ (self,num_users,dataset=None):
        """ Initialize a Swarm of users. 

        Args:
            num_users (int): number of users to be created.
            dataset (string, optional): the name of the dataset. Defaults to None.
        """
        self.num_users = num_users
        self.dataset_name = dataset
        self.dataset = self.get_whole_dataset()
        self.usernames = self.create_usernames()
        self.users = self.generate_users()
        self.swarm_name = "Swarm-"+generate_username(1)[0]
        self.TOPIC_CONTROL = ["topic-control"]
        self.IP_CONF = os.environ['IP_CONF']
        self.IP_WEB = os.environ['IP_WEB']
        self.bootstrap_server = f"{self.IP_CONF}:9094"
        self.swarm_producerConfig = {'bootstrap.servers':self.bootstrap_server,'client.id': self.swarm_name,"message.max.bytes": 1000000000,'retries':3,'retry.backoff.ms':5000,'linger.ms': 250,'acks':'all','enable.idempotence':True,"compression.type":'gzip'}
        self.swarm_consumerConfig = {'bootstrap.servers':self.bootstrap_server,'group.id': self.swarm_name,'enable.auto.commit': False, 'auto.offset.reset': 'latest','retries':3,'retry.backoff.ms':5000,'linger.ms': 250}
        self.swarm_producer = Producer(self.swarm_producerConfig)
        self.swarm_consumer = Consumer(self.swarm_consumerConfig)
        self.swarm_consumer.subscribe(self.TOPIC_CONTROL) 
                
    def create_usernames(self):
        """This method creates usernames for a specific number of user_numbers.

        Returns:
            users (list): list of randomly generated usernames.
        """
        users = []
        for i in range(self.num_users):
            one_user = generate_username(1)[0]+generate_username(1)[0]
            users.append(one_user)
        return users
    
    def generate_users(self):
        """This method uses a list of user to call User class to generate processeses for Federated Learning clients.

        Returns:
            users (list[mp.process]) : list of client processes.
        """
        users = []
        for i in range(self.num_users):
            users.append(User(username= self.usernames[i],password=self.usernames[i],service=self.dataset_name))
        return users
    
    def wait_control(self):
        """ This method waits for a message from the server to be received start the federated learning process.
        Raises:
            KafkaException: raised when the message is not correctly received.
        """
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
        """This method returns a set of training and testing datasets.

        Returns:
            train_images(numpy.array), train_labels(numpy.array), test_images(numpy.array), test_labels(numpy.array): returns training and testin dataset.
        """
        if self.dataset_name=="CIFAR-10":
            (train_images, train_labels), (test_images, test_labels) = datasets.cifar10.load_data()
        elif self.dataset_name=="MNIST" :
            (train_images, train_labels), (test_images, test_labels) = datasets.mnist.load_data()
        return (train_images, train_labels), (test_images, test_labels)
    
    def set_user_datasets(self):
        """This method sets the user dataset based on the training indices obtained from the web server.
        """
        for user in self.users:
            train_index, test_index = user.get_dataset_index()
            selected_train_images, selected_test_images = self.dataset[0][0][train_index] / 255.0, self.dataset[1][0][test_index] / 255.0
            selected_train_labels, selected_test_labels = self.dataset[0][1][train_index] , self.dataset[1][1][test_index]
            user.set_dataset([(selected_train_images, selected_train_labels), (selected_test_images, selected_test_labels)])        
    

    def run_fed(self):
        """This function creates multi-process for the users.
        """
        try:
            for user in self.users:
                    print(f"Starting user : {user.username} process.")
                    user.start()
            for user in self.users:
                    print(f"Joining user: {user.username}.")
        except:
            print("Exception in the process start/join!")

            
            
    def describe(self):
        """This method describes the swarm. For example, num_users, usernames, and dataset name.
        """
        print(f"The number of users generated is : {self.num_users}")
        print(f"The generated usernames are : {self.usernames}")
        print(f"Downloaded dataset is {self.dataset_name}")
    
    def __len__ (self):
        """This method returns the length of the swarm, i.e. number of users.

        Returns:
            _type_: _description_
        """
        return len(self.users)
    
    
    
