#Using python
FROM python:3.13-slim

ENV DASH_DEBUG_MODE False

# Default arguments for user creation
ARG USERNAME=plotly
ARG USER_UID=30000
ARG USER_GID=$USER_UID

# Create the user with UID 30000
RUN adduser --disabled-password --home /home/$USERNAME --uid $USER_UID $USERNAME

# Set the default user
USER root

# Install packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc \
    locales \
    gnupg2 \
    sudo \
    bash \
    nano \
    git \
    net-tools \
    mc \
    pkg-config \
    gdal-bin libgdal-dev \
    build-essential \
    gcc \
    postgresql-client \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Configure and generate the en_US.UTF-8 locale
RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    dpkg-reconfigure --frontend=noninteractive locales && \
    update-locale LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 LANGUAGE=en_US.UTF-8 && \
    echo 'LC_ALL="en_US.UTF-8"' >> /etc/default/locale && \
    echo 'LANG="en_US.UTF-8"' >> /etc/default/locale && \
    echo 'LANGUAGE="en_US.UTF-8"' >> /etc/default/locale

# Allow the non-root user to use sudo without password
RUN usermod -aG sudo plotly \
    && echo "$USERNAME ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Create project directories and set permissions
RUN mkdir -p /app /app/logs /app/secrets \
    && chown $USER_UID:$USER_GID /app /app/logs /app/secrets \
    && chmod -R 755 /app /app/logs \
    && chmod -R 511 /app/secrets
# The secrets folder must have 511 permissions to allow dockeruser to access the files

# Install required python packages
COPY  ./app/requirements.txt ./requirements.txt
RUN set -ex && \
    pip install -r ./requirements.txt

# Copy app files to image
USER plotly

WORKDIR /app

COPY --chown=plotly:plotly ./app /app/

# Set up entrypoint etc
EXPOSE 8050
CMD ["gunicorn", "-b", "0.0.0.0:8050", "--workers", "4", "--timeout", "120", "--reload", "app:server"]
#CMD ["/bin/bash"]