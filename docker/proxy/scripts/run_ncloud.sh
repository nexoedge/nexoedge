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
echo "Run Proxy now. My IP is ${my_ip}."

# run proxy as ncloud
proxy ${config_root_dir}
