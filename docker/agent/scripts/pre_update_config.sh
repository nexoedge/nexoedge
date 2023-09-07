#!/bin/bash

source common.sh

pre_update_proxy_list 

# generate the container list
num_container_var_name="NCLOUD_AGENT_Agent_Num_containers"
sec=$(extract_section ${num_container_var_name:13})
key=$(extract_key ${num_container_var_name:13})
sample_file=$(get_sample_file "${agent_config_file}")
default_value=$(get_ncloud_field "${sec}" "${key}" "${sample_file}")

if [ ! -z ${!num_container_var_name} ] && [ $((default_value)) -lt $((${!num_container_var_name})) ]; then
  # new line
  echo "" >> ${agent_config_file}
  for c in $(seq -f "%02g" $((default_value+1)) $((${!num_container_var_name}))); do
    # new entry
    sed -n "/^\[container01\]/,/^\[/p" "${sample_file}" | sed "s/container01/container${c}/" | sed "s/^\[container02\]//" >> ${agent_config_file}
  done
fi
