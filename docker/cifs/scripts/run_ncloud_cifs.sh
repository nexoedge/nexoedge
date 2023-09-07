#!/bin/bash

source common.sh

# prepare for ncloud configuration updates
bash pre_update_config.sh

# update ncloud configurations
bash update_config.sh

# prepare the environment
bash prepare_env.sh
bash prepare_env_cifs.sh

# report information to log
my_ip=$(get_my_ip)
echo "Run CIFS now. My IP is ${my_ip}."

# run cifs (in foregroup with log flushed to stdout)
${cifs_root_dir}/sbin/smbd -S -F
