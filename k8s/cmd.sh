#!/bin/bash

namespace=nexoedge

kind_cluster_config=kind-cluster.yaml
kind_cluster_name=nexoedge-k8s-example
kind_context=kind-${kind_cluster_name}

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

WARN() {
  echo -e "${RED}$1${NC}"
}

OKAY() {
  echo -e "${GREEN}$1${NC}"
}

INFO() {
  echo -e "${CYAN}$1${NC}"
}

resources=(
  # secrets
  "nexoedge-secrets.yaml"

  # configmap
  "nexoedge-proxy-configmap.yaml"
  "nexoedge-agents-configmap.yaml"
  "nexoedge-cifs-configmap.yaml"

  # service
  "nexoedge-svc.yaml"
)

volumes=(
  # persistent volume and their claims
  "nexoedge-metastore-pv.yaml"
  "nexoedge-metastore-pvc.yaml"
  "nexoedge-agents-pv.yaml"
  "nexoedge-agents-pvc.yaml"
  "nexoedge-cifs-pv.yaml"
  "nexoedge-cifs-pvc.yaml"
)

deployments=(
  # deployment
  "nexoedge-metastore-deployment.yaml"
  "nexoedge-proxy-deployment.yaml"
  "nexoedge-agent-1-deployment.yaml"
  "nexoedge-agent-2-deployment.yaml"
  "nexoedge-cifs-deployment.yaml"
)

create_kind_cluster() {
  kind create cluster --config ${kind_cluster_config} --name ${kind_cluster_name}
}

destroy_kind_cluster() {
  kind delete cluster --name ${kind_cluster_name}
}

create_volume_directories() {
  INFO "> Creating volume directories ..."
  path=$(grep path *-pv.yaml | sed "s/^.*: \(.*\)/\1/")
  for p in ${path[@]}; do
    echo $p
    sudo mkdir -p ${p} 
    sudo chmod 777 ${p} 
  done
}

create_namespace() {
  INFO "> Creating namespace (${namespace}) ..."
  kubectl delete namespaces ${namespace} --context=${kind_context}
  kubectl create namespace ${namespace} --context=${kind_context}
}

replace_namespace_in_yaml() {
  INFO "> Updating namespace (${namespace}) in yaml files ..."
  sed -i "s/\(.*namespace:\).*/\1 ${namespace}/" *.yaml
}

cleanup() {
  WARN "> Going to clean the data!! Confirm to proceed? (y/N)"
  read 
  while [ -z ${REPLY} ]; do
    WARN "Confirm to proceed? (y/N)"
    read
  done
  if [ ${REPLY} != "y" ]; then
    INFO "Abort data cleaning."
    return 1
  fi
  term
  INFO "> Removing the persistent volumes and other resources..."
  for ((idx=${#volumes[@]}-1; idx >= 0; idx--)); do 
    kubectl delete -f ${volumes[$idx]} --context=${kind_context}
  done
  for ((idx=${#resources[@]}-1; idx >= 0; idx--)); do 
    kubectl delete -f ${resources[$idx]} --context=${kind_context}
  done
  destroy_kind_cluster
  return 0
}

term() {
  INFO "> Terminating the deployments..."
  for ((idx=${#deployments[@]}-1; idx >= 0; idx--)); do 
    kubectl delete -f ${deployments[$idx]} --context=${kind_context}
  done
}

create() {
  cleanup
  create_kind_cluster
  if [ $? -ne 0 ]; then
    return 1
  fi
  replace_namespace_in_yaml
  create_volume_directories
  create_namespace
  INFO "> Creating the persistent volumes and other resources ..."
  for d in ${resources[@]}; do 
    kubectl apply -f ${d} --context=${kind_context}
  done
  for d in ${volumes[@]}; do 
    kubectl apply -f ${d} --context=${kind_context}
  done
  start
}

start() {
  term
  INFO "> Starting the deployments ..."
  for d in ${deployments[@]}; do 
    kubectl apply -f ${d} --context=${kind_context}
  done
}

validate() {
  for d in ${deployments[@]}; do
    kubectl apply --validate=true --dry-run=client -f ${d} --context=${kind_context}
  done
  for d in ${volumes[@]}; do
    kubectl apply --validate=true --dry-run=client -f ${d} --context=${kind_context}
  done
  for d in ${resources[@]}; do
    kubectl apply --validate=true --dry-run=client -f ${d} --context=${kind_context}
  done
}

check() {
  if [ $# -lt 1 ]; then
    # deployments
    INFO "== Deployments =="
    kubectl get deployments --namespace=${namespace} --context=${kind_context}
    echo ""
    # pods
    INFO "== Pods =="
    kubectl get pods --namespace=${namespace} --context=${kind_context}
    echo ""
    # pv
    INFO "== Persistent volumes =="
    kubectl get pv --namespace=${namespace} --context=${kind_context}
    echo ""
    # pvc
    INFO "== Persistent volume claims =="
    kubectl get pvc --namespace=${namespace} --context=${kind_context}
    echo ""
    # services
    INFO "== Services =="
    kubectl get services --namespace=${namespace} --context=${kind_context}
    echo ""
    # endpoints
    INFO "== Endpoints =="
    kubectl get endpoints --namespace=${namespace} --context=${kind_context}
    echo ""
    # configmap
    INFO "== ConfigMaps =="
    kubectl get configmaps --namespace=${namespace} --context=${kind_context}
    echo ""
    # secrets
    INFO "== Secrets =="
    kubectl get secrets --namespace=${namespace} --context=${kind_context}
    echo ""
  else
    kubectl get $1 --namespace=${namespace} --context=${kind_context}
  fi
}

log() {
  kubectl logs deploy/${1} $2 --namespace=${namespace} --context=${kind_context}
}

usage() {
  echo "Usage: $0 <create|start|term|clean|validate|check|log>"
  echo "  create: clean up all existing deployments and resources and create new ones"
  echo "  start: terminate the existing deployments and start new ones"
  echo "  term: terminate the existing deployments"
  echo "  clean: clean up all existing deployments and resources"
  echo "  validate: validate yaml files of all existing deployments and resources"
  echo "  check: show all existing deployments and resources"
  echo "  test: test the CIFS storage by uploading and downloading files"
  echo "  log <deployment-name> <container-name>: show the log of a container in a deployment"
}

# check if the commands 'kind' and 'kubectl' are available
if [ -z "$(command -v kind)" ] || [ -z "$(command -v kubectl)" ]; then
  WARN "Please install 'kind' and 'kubectl' before running this example!"
  exit 1
fi

if [ $# -ge 1 ] && [ $1 == "create" ]; then
  create
elif [ $# -ge 1 ] && [ $1 == "start" ]; then
  start
elif [ $# -ge 1 ] && [ $1 == "term" ]; then
  term
elif [ $# -ge 1 ] && [ $1 == "clean" ]; then
  cleanup
elif [ $# -ge 1 ] && [ $1 == "validate" ]; then
  validate
elif [ $# -ge 1 ] && [ $1 == "check" ]; then
  check $2
elif [ $# -ge 1 ] && [ $1 == "test" ]; then
  ./upload-download-test.sh
elif [ $# -ge 2 ] && [ $1 == "log" ]; then
  # 2 = deployment name; 3 = container name;
  log $2 $3
else
  usage
fi

