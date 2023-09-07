#!/bin/bash

BASE_DIR=$(dirname $(readlink -f $0))
source ${BASE_DIR}/common.sh # ${TAG}, ${DOCKER}, colored console log

#eval $(minikube docker-env)

# core engine
BASE_PACKAGE=nexoedge-amd64-full.deb
PROXY_PACKAGE=nexoedge-amd64-proxy.deb
AGENT_PACKAGE=nexoedge-amd64-agent.deb

# file systems
CIFS_PACKAGE=nexoedge-cifs.tar.gz

# status monitoring
UTILS_PACKAGE=nexoedge-amd64-utils.deb
ADMIN_PORTAL_PACKAGE=nexoedge-admin-portal.tar.gz

# specific test
mandate_component=$1

build_args=""

pre_fs_build() {
  tmp_dir=/tmp/proxy-extract
  parent_dir=/usr/lib
  libraries=("libncloud_zmq.so" "libzmq.so" "libzmq.so.5" "libzmq.so.5.2.5")

  # check if the proxy package exists
  if [ ! -f ${BASE_DIR}/${PROXY_PACKAGE} ]; then
    error ">> Please download the package ${PROXY_PACKAGE} before image (CIFS) build !"
    return 1
  fi

  # extract the proxy package
  dpkg -x ${BASE_DIR}/${PROXY_PACKAGE} ${tmp_dir}
  if [ $? -ne 0 ]; then
    error "Failed to extract proxy package for CIFS"
    return 1
  fi

  # move necessary libraries to cifs directory for build
  for l in ${libraries[@]}; do 
    cp ${tmp_dir}/${parent_dir}/${l} "${BASE_DIR}/cifs"
    if [ $? -ne 0 ]; then
      error "Cannot find library ${l} in ${tmp_dir}/${parent_dir} for CIFS"
      return 1
    fi
  done

  # clean up
  rm -r ${tmp_dir}
}

build_image() {
  if [ ! -f ${BASE_DIR}/${3} ]; then
    error ">> Please download the package ${3} before image (${2}) build !"
    return 1
  fi

  # go into the sub-directory,
  # clean any left-over package and copy the one from root directory,
  # copy scripts into it,
  # and start the build
  cd "${BASE_DIR}/${1}" && \
    rm -f "${3}" && \
    cp "${BASE_DIR}/${3}" . && \
    cp "${BASE_DIR}"/common/scripts/*.sh scripts/ && \
    ${DOCKER} build \
      ${build_args} \
      -t "${2}:${TAG}" \
      --build-arg PACKAGE="${3}" \
      . && \
    rm -f "${3}" && \
    cd -
}

build_image_no_scripts() {
  if [ ! -f ${BASE_DIR}/${3} ]; then
    error ">> Please download the package ${3} before image (${2}) build !"
    return 1
  fi

  cd "${BASE_DIR}/${1}" && \
    rm -f "${3}" && \
    cp "${BASE_DIR}/${3}" . && \
    ${DOCKER} build \
      ${build_args} \
      -t "${2}:${TAG}" \
      --build-arg PACKAGE="${3}" \
      . && \
    rm -f "${3}" && \
    cd -
}

build_proxy() {
  build_image proxy nexoedge-proxy ${PROXY_PACKAGE} || exit 1
}

build_agent() {
  build_image agent nexoedge-agent ${AGENT_PACKAGE} || exit 1
}

build_cifs() {
  build_image cifs nexoedge-cifs ${CIFS_PACKAGE} || if [ "${mandate_component}" = "cifs" ]; then exit 1; fi
}

build_reporter() {
  build_image reporter nexoedge-reporter ${UTILS_PACKAGE}
}

build_admin_portal_frontend() {
  build_image_no_scripts admin-portal nexoedge-admin-portal ${ADMIN_PORTAL_PACKAGE}
}

build_admin_portal_backend() {
  build_image_no_scripts webdis nexoedge-admin-portal-webdis ${ADMIN_PORTAL_PACKAGE}
}

# ncloud proxy and agent
build_proxy
build_agent
# preparation for CIFS builds
pre_fs_build
# cifs
build_cifs
# ncloud reporter
build_reporter
## ncloud admin-portal
build_admin_portal_frontend
build_admin_portal_backend

${DOCKER} image list
