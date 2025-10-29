#!/bin/bash

# Stop Data Platform
echo "Stopping Data Platform..."

docker-compose down -v

echo "Data Platform stopped and volumes cleaned up."