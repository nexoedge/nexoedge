##############
## HOME DIR ##
##############

ncloud_root_dir=/usr/lib/ncloud
cifs_root_dir=/usr/local/samba


################
## CONFIG DIR ##
################

config_root_dir=${ncloud_root_dir}/current
sample_config_root_dir=${ncloud_root_dir}/sample


##################
## CONFIG FILES ##
##################

proxy_config_file=${config_root_dir}/proxy.ini
general_config_file=${config_root_dir}/general.ini
agent_config_file=${config_root_dir}/agent.ini
storage_class_config_file=${config_root_dir}/storage_class.ini
redis_server_config_file=/etc/redis/redis.conf
cifs_config_file=${cifs_root_dir}/etc/smb.conf


#######################
## CONFIG PROCESSING ##
#######################

# file name processing
get_sample_file(){
  echo ${sample_config_root_dir}/$(echo "$1" | sed "s/^.*\/\(.*\.ini\)/\1/")
}

# variable name processing
extract_section() {
  # expected format <SECTION as [A-Z][a-z0-9_]*>_<KEY as [A-Z][a-z0-9_]*>
  #                 ^ extract this part        ^
  # and transform to lower case
  echo $1 | sed 's/\([A-Z][a-z0-9_]*\)_.*$/\L\1/'
}

extract_key() {
  # expected format <SECTION as [A-Z][a-z0-9_]*>_<KEY as [A-Z][a-z0-9_]*>
  #                                              ^ extract this part    ^
  # and transform to lower case
  echo $1 | sed 's/[A-Z][a-z0-9_]*_\(.*\)/\L\1/'
}

# field retrieval / update
get_ncloud_field() {
  sed -n "/^\[$1\]/,/^\[/ s%^$2 = \(.*\)%\1%p" "$3"
}

update_ncloud_field() {
  if [ ! -f ${4} ]; then
    return 1
  fi
  # only update if the config is to be overriden by the environment variable 
  echo "In section [$1] of config $4, update $2 = $3"
  sed -i "/^\[$1\]/,/^\[/ s%^\($2 =\).*%\1 $3%" "$4"
}

reset_ncloud_field() {
  if [ ! -f ${3} ]; then
    return 1
  fi
  # set the field to default value
  sample_file=$(get_sample_file "$3")
  default_value=$(get_ncloud_field "$1" "$2" ${sample_file})
  echo "In section [$1] of config $3, reset $2 = ${default_value}"
  update_ncloud_field "$1" "$2" "${default_value}" "$3"
}

# high-level field operations
extract_and_update_ncloud_field() {
  # 1: variable name; 2: starting offset of section and key in variable name; 3: config file to update
  section=$(extract_section ${1:$2})
  key=$(extract_key ${1:$2})
  if [ ! -z "$key" ] && [ ! -z "$section" ] && [ ! -z "${!1}" ]; then
    # if the value refers to another variable (with pattern '${.*}'), then use the value of the referred variable
    if [[ ${!1} =~ ^\$\{.*\}$ ]]; then
      l=${!1:2:-1}
      update_ncloud_field ${section} ${key} "${!l}" "${3}"
    else
      update_ncloud_field ${section} ${key} "${!1}" "${3}"
    fi
  else
    reset_ncloud_field ${section} ${key} "${3}"
  fi
}

update_proxy_field() {
  if [ ! -f ${proxy_config_file} ]; then
    return 1
  fi
  extract_and_update_ncloud_field "$1" 13 ${proxy_config_file}
}

update_agent_field() {
  if [ ! -f ${agent_config_file} ]; then
    return 1
  fi
  extract_and_update_ncloud_field "$1" 13 ${agent_config_file}
}

update_general_field() {
  if [ ! -f ${general_config_file} ]; then
    return 1
  fi
  extract_and_update_ncloud_field "$1" 15 ${general_config_file}
}

update_storage_class_field() {
  if [ ! -f ${storage_class_config_file} ]; then
    return 1
  fi
  extract_and_update_ncloud_field "$1" 20 ${storage_class_config_file}
}

update_cifs_field() {
  sec=$(extract_section ${1:12})
  key=$(extract_key ${1:12})
  echo "update (cifs) [${sec}] ${key} = ${!1} in ${cifs_config_file}"
  update_ncloud_field ${sec} ${key} ${!1} ${cifs_config_file}
}


###########
## UTILS ##
###########

get_my_ip() {
  ifconfig eth0 | awk -F ":| " '/inet / {print $10}'
}


########################
## UPDATE PREPARATION ##
########################

pre_update_proxy_list() {
  if [ ! -f ${general_config_file} ]; then
    return 1
  fi
  # generate enough entries in the proxy list
  num_proxy_var_name="NCLOUD_GENERAL_Proxy_Num_proxy"
  sec=$(extract_section ${num_proxy_var_name:15})
  key=$(extract_key ${num_proxy_var_name:15})
  sample_file=$(get_sample_file "${general_config_file}")
  default_value=$(get_ncloud_field "${sec}" "${key}" "${sample_file}")

  if [ ! -z ${!num_proxy_var_name} ] && [ $((default_value)) -lt $((${!num_proxy_var_name})) ]; then
    for c in $(seq -f "%02g" $((default_value+1)) $((${!num_proxy_var_name}))); do
      # new line
      echo "" >> ${general_config_file}
      # new entry
      sed -n "/^\[proxy01\]/,/^\[/p" "${sample_file}" | sed "s/proxy01/proxy${c}/" >> ${general_config_file}
    done
  fi
}


#####################
## ENV PREPARATION ##
#####################

prepare_log_dir() {
  if [ ! -f ${general_config_file} ]; then
    return 0
  fi
  # create log directory
  log_dir=$(sed -n "/^\[log\]/,/^\[/ s%^glog_dir = \(.*\)%\1%p" ${general_config_file})
  echo "Log dir: ${log_dir}"
  if [ ! -z ${log_dir} ] && [ ! -d ${log_dir} ]; then
    mkdir ${log_dir}
    chown ncloud:0 ${log_dir} && chmod g=u ${log_dir}
  fi
}
