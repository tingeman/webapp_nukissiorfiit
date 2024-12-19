# Description: Dockerfile for the Nginx container
FROM nginx:latest

# create a user with a custom uid
RUN adduser --disabled-password --no-create-home --uid 30000 nginx_user

# change the ownership of the Nginx directories to the new user
RUN mkdir -p /etc/nginx/ssl/ \
    && mkdir -p /usr/share/nginx/staticfiles/ \
    && chown -R nginx_user:nginx_user /var/cache/nginx/ /var/run/ /var/log/nginx/ /etc/nginx/ssl/ /usr/share/nginx/html/ /usr/share/nginx/staticfiles/ \
    && chmod 777 /usr/share/nginx/html/ /usr/share/nginx/staticfiles/

# create a self-signed certificate
RUN openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -subj '/CN=localhost' \
    -keyout /etc/nginx/ssl/nginx.key \
    -out /etc/nginx/ssl/nginx.crt \
    && chmod 644 /etc/nginx/ssl/nginx.key

# copy the nginx configuration file
COPY nginx.conf /etc/nginx/nginx.conf

# switch to the new user
USER nginx_user