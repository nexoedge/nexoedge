FROM node:16-alpine
MAINTAINER Helen Chan <hwchan@cse.cuhk.edu.hk>

# adopted from zoidbergwill (https://github.com/zoidbergwill/docker-create-react-app/blob/master/Dockerfile)

# add and install packages
ARG PACKAGE=ncloud-admin-portal.tar.gz
ADD ${PACKAGE} /

# admin-portal directory
WORKDIR /ncloud-admin

# update portal config for webdis
RUN sed -i "s%\(\"backendAddress\": \"\).*\(\".*\)%\1/webdis\2%" src/config.json

# We don't need to do this cache clean, I guess it wastes time / saves space: https://github.com/yarnpkg/rfcs/pull/53
#RUN set -ex; \
#  apk add --no-cache git; \
#  yarn install --production; \ 
#  yarn add recompose/compose && \
#  yarn run build; \
#  yarn cache clean; \
RUN set -ex; \
  npm install --production; \ 
  yarn run build; \
  yarn cache clean;


FROM nginx:alpine
WORKDIR /usr/share/nginx/html
COPY --from=0 /ncloud-admin/build/ /usr/share/nginx/html

ENV WEBDIS_REDIRECT_ADDR localhost:7379
# add script to add reverse proxy to nginx conf
ADD update_site_config.sh /docker-entrypoint.d/00-update-site-config.sh
RUN chmod +x /docker-entrypoint.d/00-update-site-config.sh
