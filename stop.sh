#!/bin/bash
docker-compose -f docker-compose-dev.yaml stop -t 20 
docker-compose -f docker-compose-dev.yaml down --remove-orphans
