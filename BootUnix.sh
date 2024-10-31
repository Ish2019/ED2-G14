#!/bin/bash
docker pull rabbitmq:latest 
docker pull python:3.10-slim

# Start the Docker Compose services
sudo docker-compose -f "docker-compose.yml" up -d --build 

# Get the container ID of the setup-python service
container_id=$(docker-compose -f "docker-compose.yml" ps -q setup-python)


# If there's no container ID, there might be an error. Exit the script.
if [[ -z "$container_id" ]]; then
  echo "Error: Could not get the container ID of the setup-python service."
  exit 1
fi

# Monitor the python_app container
while : ; do
  a= 1
  echo "Waiting for setup-python container to complete..."
  echo $a
  status=$(docker inspect --format '{{.State.Status}}' $container_id)
  if [[ "$status" != "running" ]]; then
    echo "setup-python container has completed. Removing..."
    docker rm -f  $container_id
    break
  fi
  a = $a + 1
  sleep 5
done
