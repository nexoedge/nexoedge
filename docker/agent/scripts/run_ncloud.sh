#!/bin/bash

source common.sh

# prepare for ncloud configuration updates
bash pre_update_config.sh

# update ncloud configurations
bash update_config.sh

# prepare the environment
bash prepare_env.sh

# report information to log
my_ip=$(get_my_ip)
echo "Run Agent now. My IP is ${my_ip}."

agent_ip=$(get_ncloud_field "agent" "ip" "${agent_config_file}")
if [ "${agent_ip}" == "127.0.0.1" ]; then
  update_ncloud_field "agent" "ip" "${my_ip}" "${agent_config_file}"
fi

# run proxy as ncloud
agent ${config_root_dir}
