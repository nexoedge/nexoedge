#!/bin/bash

source common.sh

main() {
  # expected format NCLOUD_<COMPONENT>_<SECTION as [A-Z][a-z0-9_]*>_<KEY as [A-Z][a-z0-9_]*>
  for var_name in $(compgen -A variable | grep ^NCLOUD_); do
    if [[ $var_name =~ ^NCLOUD_NFS_ ]]; then         # nfs
      echo "Not supported NFS config $var_name"
    elif [[ $var_name =~ ^NCLOUD_CIFS_ ]]; then      # cifs
      update_cifs_field ${var_name}
    elif [[ $var_name =~ ^NCLOUD_PROXY_ ]]; then     # proxy
      update_proxy_field ${var_name}
    elif [[ $var_name =~ ^NCLOUD_AGENT_ ]]; then     # agent
      update_agent_field ${var_name}
    elif [[ $var_name =~ ^NCLOUD_GENERAL_ ]]; then   # general
      update_general_field ${var_name}
    elif [[ $var_name =~ ^NCLOUD_STORAGECLASS_ ]]; then     # storage class (proxy)
      update_storage_class_field ${var_name}
    fi
  done
}

main
