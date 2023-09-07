#!/bin/sh

NGINX_CONFIG=/etc/nginx/conf.d/default.conf

WEBDIS_REDIRECT_PATH=/webdis
echo ">> Update NGINX config path = ${WEBDIS_REDIRECT_PATH}, backend = ${WEBDIS_REDIRECT_ADDR}"

cat <<END >$NGINX_CONFIG
server {
  listen       80;
  server_name  localhost;

  location / {
      root   /usr/share/nginx/html;
      index  index.html index.htm;
  }

  error_page   500 502 503 504  /50x.html;
  location = /50x.html {
      root   /usr/share/nginx/html;
  }

  location ${WEBDIS_REDIRECT_PATH}/ {
    proxy_pass http://${WEBDIS_REDIRECT_ADDR}/;
  }
}
END
