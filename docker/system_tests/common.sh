base_dir=$(dirname $(readlink -f $0))

source ${base_dir}/../common.sh

# host-side configurations
host_ip=172.17.0.1
redis_host_port=57005
s3_host_port=59002
etcd_host_port=2379

proxy_container_name="my-ncloud-proxy"
agent_container_name="my-ncloud-agent"
redis_container_name="my-redis"
etcd_container_name="my-etcd"
ncloud_tag=${TAG}

# default proxy settings
proxy_cport=57002
zmq_port=59001
recovery_start_interval=120
recovery_scan_interval=60
standard_storage_class_n=4
standard_storage_class_k=2
standard_storage_class_f=1
standard_storage_class_coding="rs"

# default proxy settings for enabling S3 interface
interface=s3
s3_port=59002
s3_bucket="my-bucket"
s3_secret_key="ncloudtest"
s3_region="us-east-1"

# default agent settings
num_agents=4
num_containers=1
container_capacity=10485760000 # 10GB

# s3 operations
s3_endpoint="http://${host_ip}:${s3_host_port}"
s3_credential_file="credentials"
s3_temp_file="16MB"
s3_download_file="my-download-file"
use_pipeline_for_s3=1

target_bytes_per_container=0

cur_dir=$(pwd)

###############
# Image build # 
###############

build_image() {
  cd ${base_dir}/../ && bash ./build_image.sh && cd -
}

###############
# Proxy setup # 
###############

start_redis_on_host_docker_network() {
  info "> Start redis [${redis_container_name}] with port 6379 mapped to host port ${redis_host_port}"
  ${DOCKER} run \
      -d \
      --name=${redis_container_name} \
      -p ${redis_host_port}:6379 \
      redis
}

start_etcd_on_host_docker_network() {
  info "> Start etcd [${etcd_container_name}] with port 2379 mapped to host port ${etcd_host_port}"
  ${DOCKER} run \
      -d \
      --name=${etcd_container_name} \
      -p ${etcd_host_port}:2379 \
      -e ALLOW_NONE_AUTHENTICATION=yes \
      -e ETCD_ADVERTISE_CLIENT_URLS=http://${host_ip}:${etcd_host_port} \
      -e ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:${etcd_host_port} \
      quay.io/coreos/etcd:v3.2.17
}

start_proxy_on_host_docker_network() {
  info "> Start nCloud proxy [${proxy_container_name}] with interface ${interface} with port ${proxy_cport}/${zmq_port}/${s3_port} mapped to host port ${proxy_cport}/${zmq_port}/${s3_host_port}"
  ${DOCKER} run \
      -d \
      --name=${proxy_container_name} \
      -e NCLOUD_GENERAL_Log_Glog_to_console=1 \
      -e NCLOUD_GENERAL_Proxy01_Ip=${host_ip} \
      -e NCLOUD_GENERAL_Proxy01_Coord_port=${proxy_cport} \
      -e NCLOUD_PROXY_Metastore_Port=${redis_host_port} \
      -e NCLOUD_PROXY_Proxy_Interface=${interface} \
      -e NCLOUD_PROXY_Proxy_Zmq_interface_Port=${zmq_port} \
      -e NCLOUD_PROXY_Recovery_Trigger_start_interval=${recovery_start_interval} \
      -e NCLOUD_PROXY_Recovery_Scan_interval=${recovery_scan_interval} \
      -e NCLOUD_PROXY_Metastore_Ip=${host_ip} \
      -e NCLOUD_PROXY_S3_interface_Port=${s3_port} \
      -e NCLOUD_PROXY_S3_interface_Region=${s3_region} \
      -e NCLOUD_PROXY_S3_interface_Key=${s3_secret_key} \
      -e NCLOUD_PROXY_S3_interface_Etcd_Ip=${host_ip} \
      -e NCLOUD_PROXY_S3_interface_Etcd_Port=${etcd_host_port} \
      -e NCLOUD_PROXY_Dedup_Enabled=0 \
      -e NCLOUD_PROXY_Misc_Use_pipeline_proxy_for_s3=${use_pipeline_for_s3} \
      -e NCLOUD_STORAGECLASS_Standard_N=${standard_storage_class_n} \
      -e NCLOUD_STORAGECLASS_Standard_K=${standard_storage_class_k} \
      -e NCLOUD_STORAGECLASS_Standard_F=${standard_storage_class_f} \
      -e NCLOUD_STORAGECLASS_Standard_Coding=${standard_storage_class_coding} \
      -p ${proxy_cport}:${proxy_cport} \
      -p ${zmq_port}:${zmq_port} \
      -p ${s3_host_port}:${s3_port} \
      ncloud-proxy:${ncloud_tag}
}

###############
# Agent setup # 
###############

start_agents_on_host_docker_network() {
  for i in $(seq 1 ${num_agents}); do
    start_agent_on_host_docker_network $i $i
  done
}

start_agent_on_host_docker_network() {
  info "> Start nCloud agent [${agent_container_name}-$1] with container id ${2}, volume mapping /tmp/CT0 -> host:/tmp/CT${2}"
  container_dir=/tmp/CT${2}
  rm -rf ${container_dir} 
  mkdir -p ${container_dir} && chmod 777 ${container_dir}
  ${DOCKER} run \
      -d \
      --name=${agent_container_name}-${1} \
      -e NCLOUD_GENERAL_Log_Glog_to_console=1 \
      -e NCLOUD_GENERAL_Proxy01_Ip=${host_ip} \
      -e NCLOUD_AGENT_Agent_Num_containers=${num_containers} \
      -e NCLOUD_AGENT_Container01_id=${2} \
      -e NCLOUD_AGENT_Container01_Capacity=${container_capacity} \
      -v /tmp/CT${2}:/tmp/CT0 \
      ncloud-agent:${ncloud_tag}
}


###########################
# Proxy and agent cleanup # 
###########################

remove_proxy() {
  ${DOCKER} rm -f ${proxy_container_name} ${redis_container_name} ${etcd_container_name}
}

remove_agents() {
  for i in $(seq 1 ${num_agents}); do
    ${DOCKER} rm -f ${agent_container_name}-${i}
    rm -rf /tmp/CT${i}
  done
}

clean_up() {
  remove_proxy
  remove_agents
}

##################
# Cluster status # 
##################

show_ncloud_cluster_status() {
  wait_time=5
  echo "Wait ${wait_time} seconds before status check"
  sleep ${wait_time}
  ${DOCKER} exec ${proxy_container_name} ncloud-reporter ${config_root_dir}
}

check_repair_status() {
  count=$(${DOCKER} exec ${proxy_container_name} ncloud-reporter ${config_root_dir} | grep ${target_bytes_per_container} | wc -l)
  if [ $count -eq ${standard_storage_class_n} ]; then
    return 0;
  else
    echo "Waiting for repair (current # = ${count}, target # = ${standard_storage_class_n}, target cap = ${target_bytes_per_container})"
    return 1;
  fi
}

wait_for_repair_to_complete() {
  while [ 1 -eq 1 ]; do
    check_repair_status
    if [ $? -eq 0 ]; then
      break
    fi
    show_ncloud_cluster_status
  done
}


#################
# S3 Operations #
#################

update_s3_endpoint() {
  s3_endpoint="http://${host_ip}:${s3_host_port}"
}

prepare_s3_op_check() {
  # generate temp credential file
  echo "[default]" > ${cur_dir}/${s3_credential_file}
  echo "aws_access_key_id = ncloud" >> ${cur_dir}/${s3_credential_file}
  echo "aws_secret_access_key = ${s3_secret_key}" >> ${cur_dir}/${s3_credential_file}
  if [ $? -ne 0 ]; then
    return $?
  fi
  # generate temp file for operation check
  dd if=/dev/urandom of=${cur_dir}/${s3_temp_file} bs=1M count=16
  if [ $? -ne 0 ]; then
    return $?
  fi
}

clean_up_s3_op_check() {
  sudo rm ${cur_dir}/{${s3_credential_file},${s3_temp_file},${s3_download_file}}
}

run_s3_op() {
  ${DOCKER} run --rm -it -v ${cur_dir}:/root/.aws amazon/aws-cli --endpoint=${s3_endpoint} --region=${s3_region} $@
}

