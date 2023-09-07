FROM ubuntu:jammy
MAINTAINER Helen Chan <hwchan@cse.cuhk.edu.hk>

USER root

RUN apt update \
    && apt install -y -f --no-install-recommends \
      libboost-filesystem1.74.0 \
      libboost-system1.74.0 \
      libboost-timer1.74.0 \
      libboost-log1.74.0 \
      libboost-random1.74.0 \
      libboost-locale1.74.0 \
      libboost-regex1.74.0 \
      net-tools \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN apt update \
    && apt install -y -f --no-install-recommends \
      libssl3 \
      uuid-runtime \
      redis-server \
      libevent-2.1-7 \
      libjansson4 \
      libcurl4 \
      libcurl4-gnutls-dev \
      etcd \
      libjson-c5 \
      libglib2.0-0 \
      libgflags2.2 \
      libleveldb1d \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# expose ports used by ncloud proxy
EXPOSE 57002/tcp
EXPOSE 59001/tcp
EXPOSE 59002/tcp

# add and install packages
ARG PACKAGE=nexoedge-amd64-proxy.deb
ADD ${PACKAGE} /

# install package
RUN yes no | dpkg -i ${PACKAGE} 

# add the scripts to container
ADD scripts/*.sh /
# make ncloud the owner, root as the group owner
# allow group users to execute files as if owner
# https://docs.openshift.com/container-platform/4.6/openshift_images/create-images.html
RUN chown -R ncloud:0 /*.sh /usr/lib/ncloud/ && \
    chmod -R g=u /usr/lib/ncloud


# run ncloud
USER ncloud
CMD bash run_ncloud.sh

