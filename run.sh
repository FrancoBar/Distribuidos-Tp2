#!/bin/bash
rm -r .data/output
rm -r .data/volumes
COMPOSE_HTTP_TIMEOUT=300 docker-compose -f docker-compose-dev.yaml up --build --remove-orphans
