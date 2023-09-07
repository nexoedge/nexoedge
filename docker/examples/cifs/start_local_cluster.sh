#!/bin/bash

base_dir=$(dirname $(readlink -f $0))
host_ip=172.17.0.1
cifs_container_name="my-nexoedge-cifs"
proxy_container_name="my-nexoedge-proxy"
agent_container_name="my-nexoedge-agent"
redis_container_name="my-redis"
redis_host_port=57005

source ${base_dir}/../../common/scripts/common.sh
source ${base_dir}/../../common.sh

build_image() {
  cd ${base_dir}/../../ && bash ./build_image.sh && cd -
}

start_redis_on_host_docker_network() {
  info "> Start Redis as the metadata store"
  ${DOCKER} run \
      -d \
      --name=${redis_container_name} \
      -p ${redis_host_port}:6379 \
      redis
}

start_cifs_on_host_docker_network() {
  info "> Start Nexoedge CIFS"
  ${DOCKER} run \
      -d \
      --name=${cifs_container_name} \
      -p 445:445 \
      nexoedge-cifs:${TAG}
  cp ${base_dir}/../../cifs/smb.conf . && \
      sed -i "s/^\(ncloud:ip\).*/\1 = ${host_ip}/" smb.conf && \
      ${DOCKER} cp smb.conf ${cifs_container_name}:${cifs_config_file} && \
      rm smb.conf
  ${DOCKER} restart ${cifs_container_name}
}

start_proxy_on_host_docker_network() {
  info "> Start Nexoedge Proxy"
  ${DOCKER} run \
      -d \
      --name=${proxy_container_name} \
      -e NCLOUD_GENERAL_Log_Glog_to_console=1 \
      -e NCLOUD_GENERAL_Proxy01_Ip=${host_ip} \
      -e NCLOUD_PROXY_Metastore_Port=${redis_host_port} \
      -e NCLOUD_PROXY_Metastore_Ip=${host_ip} \
      -e NCLOUD_PROXY_Dedup_Enabled=0 \
      -p 57002:57002 \
      -p 59001:59001 \
      -p 59002:59002 \
      nexoedge-proxy:${TAG}
}

start_agent_on_host_docker_network() {
  info "> Start Nexoedge Agent"
  ${DOCKER} run \
      -d \
      --name=${agent_container_name} \
      -e NCLOUD_GENERAL_Proxy01_Ip=${host_ip} \
      -e NCLOUD_AGENT_Agent_Ip=${host_ip} \
      -p 57003:57003 \
      -p 57004:57004 \
      nexoedge-agent:${TAG}
}

show_cluster_status() {
  info "> Sleep for 5 seconds and check the system status"
  sleep 5
  ${DOCKER} exec ${proxy_container_name} ncloud-reporter ${config_root_dir}
}

create_cifs_user() {
  info "> Create a CIFS user"
  # $1 = username, $2 = password
  ${DOCKER} exec ${cifs_container_name} useradd -M ${1}
  ${DOCKER} exec ${cifs_container_name} usermod -L ${1}
  echo -e "${2}\n${2}" | ${DOCKER} exec -i ${cifs_container_name} /usr/local/samba/bin/pdbedit -a ${1} -t
}

exec_cifs_command() {
  ${DOCKER} exec -u root ${cifs_container_name} ${cifs_root_dir}/bin/smbclient //127.0.0.1/ncloud ${2} -U ${1} -c "${3}"
}

check_cifs_file_op() {
  # $1 = username, $2 = password
  file=run_ncloud_cifs.sh
  # list file; upload, download, and remove file; list file
  info "> List files"
  exec_cifs_command ${1} ${2} "ls"
  info "> Update a file"
  exec_cifs_command ${1} ${2} "put ${file}"
  info "> Download the file"
  exec_cifs_command ${1} ${2} "get ${file}"
  info "> Remove the file"
  exec_cifs_command ${1} ${2} "rm ${file}"
  info "> List files"
  exec_cifs_command ${1} ${2} "ls"
}

check_cifs() {
  username=nexoedge-cifs-user
  pass=cloudtest

  create_cifs_user ${username} ${pass}
  check_cifs_file_op ${username} ${pass}
}

clean_up() {
  ${DOCKER} container rm -f ${agent_container_name} ${cifs_container_name} ${proxy_container_name} ${redis_container_name} 
}

build_image || exit 1
clean_up
start_redis_on_host_docker_network
sleep 3
start_proxy_on_host_docker_network
start_cifs_on_host_docker_network
start_agent_on_host_docker_network
show_cluster_status
check_cifs
show_cluster_status
clean_up
