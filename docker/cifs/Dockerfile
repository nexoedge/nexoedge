FROM ubuntu:jammy
MAINTAINER Helen Chan <hwchan@cse.cuhk.edu.hk>

USER root

RUN apt update \
    && apt install -y -f --no-install-recommends \
      libffi8 \
      liblmdb0 \
      libjansson4 \
      libarchive13 \
      libacl1 \
      libldap-2.5-0 \
      libsodium23 \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# expose ports used by ncloud proxy and cifs
EXPOSE 445/tcp

# add and install package
ARG PACKAGE=nexoedge-cifs.tar.gz
# 'ADD' does extraction for .tar.gz 
ADD ${PACKAGE} /usr/local

# add cifs config file
ARG CONFIG=smb.conf
RUN mkdir -p /usr/local/samba/etc/
ADD ${CONFIG} /usr/local/samba/etc/

# add libraries for ncloud
COPY libzmq.so.5.2.5 /usr/lib/
COPY libzmq.so.5 /usr/lib/
COPY libzmq.so /usr/lib/
COPY libncloud_zmq.so /usr/lib/

# add the scripts to container
ADD scripts/*.sh /

# run ncloud
CMD bash run_ncloud_cifs.sh

