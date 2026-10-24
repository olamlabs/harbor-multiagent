FROM python:3.13-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl git \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --shell /bin/bash player
