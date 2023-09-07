FROM ubuntu:jammy
MAINTAINER Helen Chan <hwchan@cse.cuhk.edu.hk>

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
      redis-server \
      redis-tools \
      libjson-c5 \
      libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/* 

# add and install packages
ARG PACKAGE=ncloud-latest-amd64.deb
ADD ${PACKAGE} /

# install package
RUN dpkg -i ${PACKAGE} 

# add the scripts to container
ADD scripts/*.sh /
RUN chown ncloud:ncloud /*.sh 
RUN chgrp -R 0 /usr/lib/ncloud && \
    chmod -R g=u /usr/lib/ncloud

# run ncloud reporter
USER ncloud
CMD bash run_reporter.sh

