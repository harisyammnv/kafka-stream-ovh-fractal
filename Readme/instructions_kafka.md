<<<<<<< HEAD

1)First you should create a namespace for keeping your all the services and pods inside a particular namespace . You can create namespace by using below command :
kubectl create namespace kafka-strimzi
 

2)Install Strimzi Operator using helm:
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka-strimzi' -n kafka
 

3)Apply the kafka yaml file for kafka cluster
kubectl apply -f kafka.yaml -n kafka-strimzi

4)Clone the repo for helm charts.   
git clone https://github.com/confluentinc/cp-helm-charts.git


5)Install schema registry for validating the schema ,using helm ,while installing the schema registry
You have to provide external ip address with port for bootstrap.

helm install kafka-schema-registry --set kafka.bootstrapServers="PLAINTEXT://"External IP ADDRESS of bootstrap:9094" cp-helm-charts/charts/cp-schema-registry -n kafka
 
 
6)Create loadbalancer for exposing  schema registry externally.  
For that create a  yaml file using below command:
kubectl apply -f schema.yaml -n kafka-strimzi
 
 
7)Create ksql server using helm with external ip address of schema registry.
helm install ksql-server --set cp-schema-registry.url=http://External IP ADDRESS of schema registry:8081,kafka.bootstrapServers="PLAINTEXT://External IP ADDRESS of bootstrap:9094",ksql.headless=false cp-helm-charts/charts/cp-ksql-server -n kafka-strimzi


8)create another yaml file for exposing Ksql server .
kubectl apply ksql.yaml -n kafka-strimzi
 

9)For verification of our external ip address  is working fine , we will run the below command with providing our just created external ip address for Ksql.
kubectl -n kafka-strimzi run tmp-ksql-cli1 --rm -i --tty --image confluentinc/cp-ksql-cli:5.2.1 http://External IP ADDRESS of ksql:8088
 


