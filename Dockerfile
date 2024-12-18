#Using python
FROM python:3.12-slim

ENV DASH_DEBUG_MODE False

# Create the plotly user with UID 30000
RUN adduser --disabled-password --uid 30000 plotly

# Update and install relevant packages
RUN apt-get update && apt-get install -y sudo curl nano wget unzip git   \
    && usermod -aG sudo plotly \
    && echo "%sudo ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Install required python packages
COPY  ./app/requirements.txt ./requirements.txt
RUN set -ex && \
    pip install -r ./requirements.txt

# Copy app files to image
USER plotly
WORKDIR /var/www/app
COPY --chown=plotly:plotly ./app /var/www/app/

# Set up entrypoint etc
EXPOSE 8050
CMD ["gunicorn", "-b", "0.0.0.0:8050", "--reload", "app:server"]
#CMD ["/bin/bash"]