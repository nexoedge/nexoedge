#!/bin/bash

base_dir=$(dirname $(readlink -f $0))

source ${base_dir}/common.sh

# proxy settings
recovery_start_interval=5
recovery_scan_interval=5
standard_storage_class_n=9
standard_storage_class_k=6
standard_storage_class_f=1
standard_storage_class_coding="rs"

# agent settings
num_agents=3
num_containers=3
container_capacity=10485760000 # 10GB

target_bytes_per_container=2796203

# for getting the latest ${config_dir} in containers
source ${base_dir}/../common/scripts/common.sh

# redefine for specific deployment
start_agent_on_host_docker_network() {
  ${DOCKER} run \
      -d \
      --name=${agent_container_name} \
      -e NCLOUD_GENERAL_Log_Glog_to_console=1 \
      -e NCLOUD_GENERAL_Proxy01_Ip=${host_ip} \
      -e NCLOUD_AGENT_Agent_Num_containers=${num_containers} \
      -e NCLOUD_AGENT_Container01_id=${1} \
      -e NCLOUD_AGENT_Container01_Capacity=${container_capacity} \
      -e NCLOUD_AGENT_Container02_id=${2} \
      -e NCLOUD_AGENT_Container02_Capacity=${container_capacity} \
      -e NCLOUD_AGENT_Container03_id=${3} \
      -e NCLOUD_AGENT_Container03_Capacity=${container_capacity} \
      -v /tmp/CT${1}:/tmp/CT0 \
      -v /tmp/CT${2}:/tmp/CT1 \
      -v /tmp/CT${3}:/tmp/CT2 \
      ncloud-agent:${ncloud_tag}
}

# redefine for specific deployment
clean_up() {
  ${DOCKER} container rm -f ${proxy_container_name} ${redis_container_name} ${etcd_container_name}
  for i in $(seq 1 ${num_agents}); do
    ${DOCKER} container rm -f my-ncloud-agent-$i
  done
  clean_up_s3_op_check
  st=$((1 * num_containers))
  ed=$((num_agents * num_containers + num_containers + 2))
  for c in $(seq ${st} ${ed}); do
    rm -r -f /tmp/CT${c}
  done
}


# Main

build_image || exit 1
clean_up

# normal cluster start-up
start_redis_on_host_docker_network
start_etcd_on_host_docker_network
sleep 3
start_proxy_on_host_docker_network
for i in $(seq 1 ${num_agents}); do
  agent_container_name="my-ncloud-agent-$i"
  st=$((i * num_containers))
  ed=$((i * num_containers + num_containers + 2)) # two extra containers for repair testing
  for c in $(seq ${st} ${ed}); do
    container_dir=/tmp/CT${c}
    mkdir -p ${container_dir} && chmod 777 ${container_dir}
    rm -f ${container_dir}/*
  done
  start_agent_on_host_docker_network $(seq ${st} ${ed})
done
show_ncloud_cluster_status
show_ncloud_cluster_status
#${DOCKER} logs ${proxy_container_name}

# write some data
prepare_s3_op_check
run_s3_op s3 cp /root/.aws/${s3_temp_file} s3://${s3_bucket}/ || error "-- Failed to upload large object."

# remove one agent, and start a new one with a storage container replaced
info "> Pick a parity container for repair"
agent_container_name="my-ncloud-agent-3"
${DOCKER} container rm -f ${agent_container_name}
mkdir -p /tmp/CT13
start_agent_on_host_docker_network 9 10 12
show_ncloud_cluster_status

# wait for repair on the new storage container
wait_for_repair_to_complete

# read using repaired data
${DOCKER} container stop "my-ncloud-agent-1"
run_s3_op s3 cp s3://${s3_bucket}/${s3_temp_file} /root/.aws/${s3_download_file} || error "-- Failed to read large object."
diff ${s3_download_file} ${s3_temp_file} || error "-- Repair data is corrupted!"

# remove another agent, and start a new one with a new storage container replaced
info "> Pick a data container for repair"
agent_container_name="my-ncloud-agent-1"
${DOCKER} container rm -f ${agent_container_name}
mkdir -p /tmp/CT14
start_agent_on_host_docker_network 3 13 5
show_ncloud_cluster_status

# wait for repair again
wait_for_repair_to_complete

# read using repaired data
${DOCKER} container stop "my-ncloud-agent-3"
run_s3_op s3 cp s3://${s3_bucket}/${s3_temp_file} /root/.aws/${s3_download_file} || error "-- Failed to read large object."
diff ${s3_download_file} ${s3_temp_file} || error "-- Repair data is corrupted!"

clean_up
