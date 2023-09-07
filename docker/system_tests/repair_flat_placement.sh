#!/bin/bash

base_dir=$(dirname $(readlink -f $0))

source ${base_dir}/common.sh

# for getting the latest ${config_dir} in containers
source ${base_dir}/../common/scripts/common.sh

recovery_start_interval=5
recovery_scan_interval=5
use_pipeline_for_s3=0

write_file() {
  info ">> Write a file ${s3_temp_file} to bucket ${s3_bucket}"
  # write a file
  run_s3_op s3 cp /root/.aws/${s3_temp_file} s3://${s3_bucket}/ || error "-- Failed to upload large object."
}

test_degraded_read() {
  info ">> Stop agent 1"
  # fail one container
  sudo docker container stop ${agent_container_name}-1
  info ">> Degraded read"
  # read file under single-container failure
  run_s3_op s3 cp s3://${s3_bucket}/${s3_temp_file} /root/.aws/${s3_download_file} || error "-- Failed to read large object."
  diff ${s3_download_file} ${s3_temp_file} || error "-- Degraded read: data is corrupted!"

  # fail another container
  info ">> Stop agent 2"
  sudo docker container stop ${agent_container_name}-2
  # read file under double-container failure
  info ">> Degraded read"
  run_s3_op s3 cp s3://${s3_bucket}/${s3_temp_file} /root/.aws/${s3_download_file} || error "-- Failed to read large object."
  diff ${s3_download_file} ${s3_temp_file} || error "-- Degraded read: data is corrupted!"
}

test_repair() {
  info ">> Start agent 1, remove agent 2, and start a new agent 2 with a new storage container"
  # check single-container failure
  sudo docker container start ${agent_container_name}-1
  sudo docker container rm -f ${agent_container_name}-2
  start_agent_on_host_docker_network 2 5
  info ">> Wait for reparing to complete ..."
  wait_for_repair_to_complete
  info ">> Stop agent 1"
  sudo docker container stop ${agent_container_name}-1
  # read file
  info ">> Read using repaired data"
  run_s3_op s3 cp s3://${s3_bucket}/${s3_temp_file} /root/.aws/${s3_download_file} || error "-- Failed to read large object."
  diff ${s3_download_file} ${s3_temp_file} || error "-- Read after repairing single-container failure: data is corrupted!"

  # check double-container failure
  info "> Remove agent 1 and agent 2, and start a new agent 1 and agent 2 with a new storage container each"
  sudo docker container rm -f ${agent_container_name}-1
  sudo docker container rm -f ${agent_container_name}-2
  start_agent_on_host_docker_network 1 6
  start_agent_on_host_docker_network 2 7
  info ">> Wait for reparing to complete ..."
  wait_for_repair_to_complete
  info ">> Stop agent 3 and 4"
  sudo docker container stop ${agent_container_name}-3
  sudo docker container stop ${agent_container_name}-4
  # read file
  info ">> Read using repaired data"
  run_s3_op s3 cp s3://${s3_bucket}/${s3_temp_file} /root/.aws/${s3_download_file} || error "-- Failed to upload large object."
  diff ${s3_download_file} ${s3_temp_file} || error "-- Read after repairing double-container failure: data is corrupted!"
}

run_tests() {
  write_file
  test_degraded_read
  test_repair
}

# main

build_image || exit 1
clean_up

prepare_s3_op_check
use_pipeline_for_s3=0

# Setting 1: n=4, k=2, rs
standard_storage_class_n=4
standard_storage_class_k=2
standard_storage_class_f=2
standard_storage_class_coding="rs"
target_bytes_per_container=8388608

info "=== START ==="
info "> Check setting n=${standard_storage_class_n}, k=${standard_storage_class_k}, f=${standard_storage_class_f}, coding=${standard_storage_class_coding}"
# normal setup
start_redis_on_host_docker_network
start_etcd_on_host_docker_network
sleep 3
start_proxy_on_host_docker_network
start_agents_on_host_docker_network

show_ncloud_cluster_status
#docker logs ${proxy_container_name}

run_tests

clean_up
st=${num_agents}
ed=$((num_agents + 2))
for i in $(seq ${st} ${ed}); do
  rm -rf /tmp/CT${i}
done

# Setting 2: n=4, k=2, fmsr
standard_storage_class_n=4
standard_storage_class_k=2
standard_storage_class_f=2
standard_storage_class_coding="fmsr"
target_bytes_per_container=8388610

info "> Check setting n=${standard_storage_class_n}, k=${standard_storage_class_k}, f=${standard_storage_class_f}, coding=${standard_storage_class_coding}"
# normal setup
start_redis_on_host_docker_network
start_etcd_on_host_docker_network
sleep 3
start_proxy_on_host_docker_network
start_agents_on_host_docker_network

show_ncloud_cluster_status
#docker logs ${proxy_container_name}

run_tests

clean_up
st=${num_agents}
ed=$((num_agents + 2))
for i in $(seq ${st} ${ed}); do
  rm -rf /tmp/CT${i}
done

info "=== END ==="
