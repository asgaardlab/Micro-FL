from flask import Flask, jsonify, request
from flask_restful import Api, Resource
import bcrypt
import json
import elasticsearch
from elasticsearch import Elasticsearch , RequestError
from elasticsearch_dsl import Search
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
from numpyencoder import NumpyEncoder
import numpy as np
import os 

ELASTIC_PASSWORD = os.environ['ELASTIC_PASSWORD']
bootstrap_server= "my-cluster-kafka-bootstrap:9092"
conf_admin = {'bootstrap.servers': bootstrap_server}
admin = AdminClient({'bootstrap.servers': bootstrap_server})
## Defining app and the api
app = Flask(__name__)
api = Api(app)

#### Establishing ElasticSearch
# es = Elasticsearch([
#     {'host': 'es01'},
#     { 'port': 9200, 'url_prefix': 'es', 'use_ssl': True, }])
es = Elasticsearch(
    ['mikael-elasticsearch-es-internal-http'],
    http_auth=('elastic', f'{ELASTIC_PASSWORD}'),
    scheme="http",
    port=9200,
)

def json_serializer(data):
    return json.dumps(data,cls=NumpyEncoder)


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

es_create_index_if_not_exists(es, "users") # Creates the "users" index ; doesn't fail if it already exists
es_create_index_if_not_exists(es, "users_status") # Creates the "users" index ; doesn't fail if it already exists


def check_connection(elastic_server):
    if elastic_server.ping():
        print("The connection was successful!")
        resp = elastic_server.info()
        print(json.dumps(resp,indent=4, sort_keys=True))
    else:
        print("Connection Error!")

def UserExist(username):
    s = Search(using=es,index="users",doc_type='doc')
    # s = s.filter("term",username=username)
    s = s.query('term',username__keyword=username)
    response= [x for x in s.scan()]
    if len(response)==0:
        return False
    else:
        return True

def login_validation(username,password):
    s = Search(using=es,index="users",doc_type='doc')
    s = s.query("match",username__keyword=username)
    response= [x for x in s.scan()]
    db_pw = response[0]['password']
    # hashed_pw = bcrypt.hashpw(password.encode('utf8'), bcrypt.gensalt()).decode('utf8')
    # print(db_pw)
    # print(hashed_pw)
    if bcrypt.hashpw(password.encode('utf8'), db_pw.encode('utf8')).decode('utf8') == db_pw:
        return True
    else:
        return False


def get_dataset_index(es,username):
    s = Search(using=es,index="datasets",doc_type='doc')
    s = s.query("match",username__keyword=username)
    response= [x for x in s.scan()]
    # if len(response)==0:
    #     return False
    # else: 
    #     return response[0]
    return response[0]

check_connection(es)
class Register(Resource):
    def post(self):
        #If I am here, then the resouce register was requested using the method POST
        #Step 1: Get posted data:
        postedData = request.get_json()
        print(postedData)
        username= postedData['username']
        password= postedData['password']
        if UserExist(username):
            retJson = {
                'status':301,
                'msg': 'Invalid Username / user already registered.'
            }
            return jsonify(retJson)
        hashed_pw = bcrypt.hashpw(password.encode('utf8'), bcrypt.gensalt()).decode('utf8')
        message = {
            'username':username,
            'password':hashed_pw
        }
        es.create(index="users",id=message['username'],body=json.dumps(message))
        retJson = {
            "status": 200,
            "msg": "You successfully signed up for the API"
        }
        return jsonify(retJson)



class Login(Resource):
    def post(self):
        #If I am here, then the resouce register was requested using the method POST
        #Step 1: Get posted data:
        postedData = request.get_json()
        print(postedData)
        username= postedData['username']
        password= postedData['password']
        subscription = postedData['MS']
        if login_validation(username=username,password=password):
            retJson={
                'status':200,
                'msg': "Login was successful.",
                'host': "localhost:29092",
                'topic_pub':"Client-Message",
                'topic_sub':"shared_model_message",
                'topic_train_event': 'topic_train_event',
                'topic_user_status': 'topic-user-status'
            }
            return jsonify(retJson)
        else:
            retJson={
                'status': 301,
                'msg': "invalid request."
            }
            return retJson

class Get_index(Resource):
    def post(self):
        #If I am here, the we are trying to first login and then if its valid we get the indexs of the dataset
        #Step 1: Get posted data:
        postedData = request.get_json()
        print(postedData)
        username= postedData['username']
        password= postedData['password']
        # subscription = postedData['MS']
        if login_validation(username=username,password=password):
            index_data = get_dataset_index(es,username)
            retJson={
                'status':200,
                'msg': "Login was successful and the indexes are attached to this.",
                "train_index": np.array(index_data["train_index"]),
                "test_index" : np.array(index_data["test_index"]),
            }
            print(retJson)
            return json_serializer(retJson)
        else:
            retJson={
                'status': 301,
                'msg': "invalid request."
            }
            return retJson

class Kafka_server(Resource):
    def post(self):
        postedData = request.get_json()
        command = postedData['command']
        username= postedData['username']
        password= postedData['password']
        if username=="Mikael" and password=="Mikael":
            if command == 'init':
                topics = list(['Client-Message','shared_model_message'])
                NewTopic(topics[0], num_partitions=3, replication_factor=2, config={'log.retention.hours': '1'})
                NewTopic(topics[1], num_partitions=3, replication_factor=2, config={'log.retention.hours': '1'})
                retJson= {
                    "status": 200,
                    "msg" : "The topics are created successfully."
                }
                return jsonify(retJson)
            elif command=='delete':
                topics = list(['Client-Message','shared_model_message'])
                admin.delete_topics(topics)
                retJson= {
                    "status": 200,
                    "msg" : "The topics are deleted successfully."
                }
                return jsonify(retJson)

            else:
                retJson= {
                    "status": 301,
                    "msg" : "Invalid request."
                }
                return jsonify(retJson)
        else:
            retJson= {
                    "status": 301,
                    "msg" : "Invalid request."
                }
            return jsonify(retJson)

class Services(Resource):
    def get(self):
        message= {'msg' : 'Welcome to the Federated Learning as a Microservice Project. Currently only CIFAR-10 Federated learning is available.'}
        return jsonify(message)













api.add_resource(Register, '/register')
api.add_resource(Services, '/')
api.add_resource(Kafka_server, '/kafka')
api.add_resource(Login, '/login')
api.add_resource(Get_index,'/get_index')





if __name__=="__main__":
    app.run(host='0.0.0.0',debug=True)
