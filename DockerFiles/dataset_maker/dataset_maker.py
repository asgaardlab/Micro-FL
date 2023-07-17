from confluent_kafka import Producer , Consumer
import socket
import json
from numpyencoder import NumpyEncoder
import create_dataset as microdata
import time

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def json_serializer(data):
    return json.dumps(data,cls=NumpyEncoder).encode('utf-8')

def start_experiment(producer,dataset,num_users):
    message = {'msg':'Start','dataset':dataset,'num_users':num_users}
    topic_control = 'topic-control'
    producer.produce(topic_control, value=json_serializer(message),callback=acked)

def init_producer():
    bootstrap_server= "my-cluster-kafka-bootstrap:9092"
    producerConfig = {'bootstrap.servers':bootstrap_server,"message.max.bytes": 1000000000,
            'client.id': socket.gethostname(),"compression.type":'gzip','retries':3,'retry.backoff.ms':5000,'linger.ms': 250,'acks':'all','enable.idempotence':True}

    producer = Producer(producerConfig)
    
    return producer

def init_consumer():
    bootstrap_server= "my-cluster-kafka-bootstrap:9092"
    consumerConfig = {'bootstrap.servers': bootstrap_server,
            'group.id': "DatasetMaker",
            'enable.auto.commit': False,
            'auto.offset.reset': 'latest',
            'retries':3,'retry.backoff.ms':5000,'linger.ms': 250,}
    consumer = Consumer(consumerConfig)
    
    return consumer


def topic_dataset(consumer):
    try:
        consumer.subscribe(["topic-dataset"])
        print("Waiting for the dataset_creation signal!")
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
                msg= message['msg']
                break
        consumer.close()
        return msg, dataset_name, num_users
    except:
        print("exception happened in control message. Closing the conumser!")
        consumer.close()



if __name__ == '__main__':
    
    while (True):
        producer = init_producer()
        consumer = init_consumer()
        es = microdata.init_database()
        msg, dataset_name , num_users = topic_dataset(consumer)
        
        if msg == 'start':
            microdata.make_dataset(es,dataset_name=dataset_name)
            time.sleep(20)
            start_experiment(producer,dataset=dataset_name,num_users=num_users)
        if msg=='stop':
            microdata.delete_userdata(es)
        
    