#!/bin/bash

# Create the necessary namespaces for Micro-FL
echo "Creating the necessary namespaces for Micro-FL..."
sudo kubectl create namespace kafka
sudo kubectl create namespace monitoring
sudo kubectl create ns chaos-mesh

# Create Kafka Clusters
echo "Creating Kafka Clusters..."
echo "----------------------------------------------------------------"
sudo kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
sudo kubectl apply -f deployment-kafka.yaml -n kafka

# Check Kafka Deployment status
echo "Checking Kafka Deployment status..."
sudo kubectl rollout status deployment my-cluster -n kafka
echo "Kafka Cluster Ready..."
echo "----------------------------------------------------------------"

# Create Kafka Topics
echo "Creating the Topics..."
sudo kubectl apply -f ./Topics/topics-deployment.yaml
echo "Topics Created..."
echo "----------------------------------------------------------------"

# Create Elasticsearch Stack
echo "Creating Elasticsearch Stack..."
echo "Creating the CRDs and Operators for Elasticsearch"
sudo kubectl create -f https://download.elastic.co/downloads/eck/2.7.0/crds.yaml
sudo kubectl apply -f https://download.elastic.co/downloads/eck/2.7.0/operator.yaml 
sudo kubectl create secret generic mikael-elasticsearch-es-elastic-user --from-literal=elastic=elasticpassword -n kafka
echo "Elasticsearch Stack password created... Username: elastic , password: elasticpassword"
echo "Deploying the Elasticsearch Cluster"
sudo kubectl apply -f deployment-elastic.yaml
sudo kubectl apply -f deployment-kibana.yaml

# Check Elasticsearch Deployment status
echo "Checking the Elasticsearch Cluster status..."
sudo kubectl rollout status statefulsets mikael-elasticsearch-es-master -n kafka 
sudo kubectl rollout status statefulsets mikael-elasticsearch-es-data -n kafka 
sudo kubectl rollout status deployment deployment-kibana.yaml -n kafka
echo "Elasticsearch Deployment Ready..."
echo "----------------------------------------------------------------"

# Create Connect Cluster
echo "Creating Connect Cluster..."
sudo kubectl apply -f Connect/deployment-connect.yaml 
sudo kubectl apply -f Connect/deployment-connector-aggregator.yaml
sudo kubectl apply -f Connect/deployment-connector-client.yaml
sudo kubectl apply -f Connect/deployment-connector-control.yaml
sudo kubectl apply -f Connect/deployment-connector-dataset.yaml
sudo kubectl apply -f Connect/deployment-connector-status.yaml
echo "Checking the Connect Cluster status..."
sudo kubectl rollout status deployment my-connect-cluster-connect -n kafka
echo "Connect cluster Ready..."
echo "----------------------------------------------------------------"

# Create the Web Deployment
echo "Creating the Web Deployment..."
sudo kubectl apply -k ./web
echo "Checking the Web Deployment status..."
sudo kubectl rollout status deployment web -n kafka
echo "Web Deployment ready..."
echo "----------------------------------------------------------------"
# Create aggregator and DatasetMaker
echo "Deploying the Aggregator and DatasetMaker..."
sudo kubectl apply -f deployment-aggregator.yaml 
sudo kubectl apply -f deployment-datasetmaker.yaml

# Check the Aggregator and DatasetMaker Status
echo "Checking the Aggregator and DatasetMaker Status..."
sudo kubectl rollout status deployment aggregator -n kafka
sudo kubectl rollout status deployment datasetmaker -n kafka
echo "Aggregator and DatasetMaker Ready..."
echo "----------------------------------------------------------------"

# Create Prometheus and Grafana
echo "Creating Prometheus and Grafana..."

# Download and modify the bundle.yaml file to specify the monitoring namespace
curl -s https://raw.githubusercontent.com/coreos/prometheus-operator/master/bundle.yaml | sed -e '/[[:space:]]*namespace: [a-zA-Z0-9-]*$/s/namespace:[[:space:]]*[a-zA-Z0-9-]*$/namespace: monitoring/' > Monitoring/prometheus-operator-deployment.yaml

# Create the Prometheus Operator deployment and ClusterRole
sudo kubectl create -f Monitoring/prometheus-operator-deployment.yaml -n monitoring
sudo kubectl apply -f Monitoring/clusterRole.yaml

# Create the deployment, additional configuration, and service for Prometheus
sudo kubectl apply -f Monitoring/strimzi-pod-monitor.yaml
sudo kubectl apply -f Monitoring/prometheus-deployment.yaml
sudo kubectl apply -f Monitoring/prometheus-additional.yaml
sudo kubectl apply -f Monitoring/prometheus-service.yaml

# Create Grafana datasource configuration, deployment, and service
sudo kubectl apply -f Monitoring/grafana-datasource-config.yaml
sudo kubectl apply -f Monitoring/grafana-deployment.yaml 
sudo kubectl apply -f Monitoring/grafana-service.yaml

# Check the status of the Monitoring deployment 
echo "Checking the status of the monitoring deployment..."
sudo kubectl rollout status statefulsets prometheus-prometheus-deployment -n monitoring
sudo kubectl rollout status deployment grafana -n monitoring
echo "Monitoring deployment Ready..."
echo "----------------------------------------------------------------"

# Craeting the Chaos-Mesh deployment
# sudo helm repo add chaos-mesh https://charts.chaos-mesh.org

# sudo helm install chaos-mesh chaos-mesh/chaos-mesh --namespace=chaos-mesh --set chaosDaemon.runtime=containerd --set chaosDaemon.socketPath=/run/containerd/containerd.sock --set dashboard.securityMode=false --version 2.5.1