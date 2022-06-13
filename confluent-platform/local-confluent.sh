# Confluent Platform for Local
curl -O http://packages.confluent.io/archive/7.1/confluent-7.1.1.zip
unzip confluent-7.1.1.
# Confluent CLI
curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest
export PATH=$(pwd)/bin:$PATH

# Confluent for K8s plugin
curl -O https://confluent-for-kubernetes.s3-us-west-1.amazonaws.com/confluent-for-kubernetes-2.3.1.tar.gz
tar -xvf kubectl-plugin/kubectl-confluent-linux-amd64.tar.gz -C /usr/local/bin