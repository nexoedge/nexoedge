#!/bin/bash

source common.sh

ls -lR /usr/lib/ncloud

# update ncloud configurations
bash update_config.sh

# prepare the environment
bash prepare_env.sh

# report information to log
my_ip=$(get_my_ip)
proxy_ip=$(get_ncloud_field proxy01 ip ${general_config_file})
echo "Run Reporter now. My IP is ${my_ip}. Proxy IP is ${proxy_ip}."

# run proxy as ncloud
ncloud-reporter -s 1 /usr/lib/ncloud/current -r
