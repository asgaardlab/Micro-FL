from microfl import User, Swarm
import time 
import os

if __name__ == '__main__':
    ## Swarm defined number of users with the dataset name
    dataset_name = os.environ['DATASET_NAME']
    swarm = Swarm(num_users=10,dataset=dataset_name)
    ## The swarm waits for the Control Signal
    swarm.wait_control()
    ## When the control signal is received this command will download the datasets for all user
    swarm.set_user_datasets()
    ## This wait time is to make sure the datasets are downloaded.
    time.sleep(20)
    ## Start the federated training
    swarm.run_fed()
