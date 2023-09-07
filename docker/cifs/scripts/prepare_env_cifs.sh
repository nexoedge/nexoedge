#!/bin/bash

source common.sh

prepare_cifs_folders() {
  for path in $(sed -n 's/path = *\(.*\)/\1/p' ${cifs_config_file}); do
    echo "Create ${path} for CIFS"
    mkdir -p ${path}
    chmod 777 ${path}
  done
}

add_cifs_users() {
  for ((i=0 ; i < ${#CIFS_USERS[@]}; i++)); do
    user=${CIFS_USERS[i]}
    pass=${CIFS_PASSWORDS[i]}
    # create user if not exists on system, but do not generate home directory and disable system login
    useradd -M ${user}
    #usermod -L ${user}
    # add user and set password for CIFS
    echo "Add user $i [${user}] to CIFS"
    echo -e "${pass}\n${pass}" | ${cifs_root_dir}/bin/pdbedit -a ${user} -t
  done
}

prepare_cifs_folders
add_cifs_users
