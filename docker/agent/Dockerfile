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
      libsodium23 \
      net-tools \  
    && rm -rf /var/lib/apt/lists/* 

RUN apt update \
    && apt install -y -f --no-install-recommends \
      libxml2 \
      libjsoncpp25 \
      libaprutil1 \
      libapr1 \
      libcurl4 \
      libcurl4-gnutls-dev \
      libglib2.0-0 \
      zlib1g \
      libre2-9 \
      libleveldb1d \
      librocksdb6.11 \
    && rm -rf /var/lib/apt/lists/* 

RUN apt update \
    && apt install -y -f --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/* 

# expose ports used by ncloud agent
EXPOSE 57003/tcp
EXPOSE 57004/tcp

# add packages
ARG PACKAGE=nexoedge-amd64-agent.deb
ADD ${PACKAGE} /

# install package
RUN yes no | dpkg -i ${PACKAGE}

# add the scripts to container
ADD scripts/*.sh /
RUN chown -R ncloud:0 /*.sh  /usr/lib/ncloud && \
    chmod -R g=u /usr/lib/ncloud

# run ncloud
USER ncloud
CMD bash run_ncloud.sh

