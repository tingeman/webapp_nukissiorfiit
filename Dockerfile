# Base image: python 3.13.18-slim, pinned by digest (index digest, multi-arch), as of
# 2026-11-09. build_image.sh builds without --build-arg, so this default is what releases
# use. It must equal NUKI_PYTHON_IMAGE in compose.pins.env (tests/run.sh checks that).
# To update: docker buildx imagetools inspect python:3.13-slim, change both, then recompile
# the lock files with scripts/compile_lock.py (see README).
#
# Stages: runtime (the production image), dev (runtime + IPython/ipdb, gunicorn --reload),
# prod (= runtime). build_image.sh runs a plain docker build (no --target), which builds the
# LAST stage, so prod must stay last: a release never contains the dev tools. Development:
# docker build --target dev, or compose.develop.yml (build target dev).
ARG PYTHON_IMAGE=python:3.13.18-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90
FROM ${PYTHON_IMAGE} AS runtime

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

# Install the python packages from the hash lock (written by scripts/compile_lock.py from
# app/requirements.in). Before COPY ./app, so a code change does not reinstall packages.
COPY  ./app/requirements.txt ./requirements.txt
RUN set -ex && \
    pip install --no-cache-dir --require-hashes -r ./requirements.txt

# Copy app files to image
USER plotly

WORKDIR /app

COPY --chown=plotly:plotly ./app /app/

# Set up entrypoint etc
EXPOSE 8050
# The Dash index page, served by the app itself (no backend API call), checked with the
# image's own python (the image has no curl). --start-interval makes a fresh container
# healthy within seconds of gunicorn being ready (docker compose up --wait).
HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --start-interval=2s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8050/app/nuki/', timeout=8)"]
CMD ["gunicorn", "-b", "0.0.0.0:8050", "--workers", "4", "--timeout", "120", "app:server"]


# ---- dev: runtime + the dev tools (app/requirements-dev.txt), auto-reload -----------------
# For local development only (compose.develop.yml mounts ./app over /app). The dev lock is
# compiled with requirements.txt as constraint, so it only adds packages and never changes
# a production version.
FROM runtime AS dev
USER root
COPY ./app/requirements-dev.txt /tmp/requirements-dev.txt
RUN set -ex && \
    pip install --no-cache-dir --require-hashes -r /tmp/requirements-dev.txt && \
    rm /tmp/requirements-dev.txt
USER plotly
# --reload: restart the worker when a .py file changes (polling, works on bind mounts from
# Windows). One worker and --timeout 0, so a breakpoint (ipdb) is not killed by the
# worker timeout; attach to it with docker attach <container>. --threads 4 (gthread worker):
# without nginx in front, a browser's idle speculative connection would block a single sync
# worker forever (no timeout); gthread waits on connections without blocking the others.
CMD ["gunicorn", "-b", "0.0.0.0:8050", "--workers", "1", "--threads", "4", "--timeout", "0", "--reload", "app:server"]


# ---- prod: the release image. Must be the LAST stage (see the top of this file). ----------
FROM runtime AS prod
