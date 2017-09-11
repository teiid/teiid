#!/bin/bash
set -e

sudo docker build --rm -t registry.hub.docker.com/teiid/teiid:${project.version} .

echo "Want to push the image to Docker Hub (y/n) ?"

read selection

if [ "$selection" == "y" ]; then
    sudo docker login registry.hub.docker.com
    sudo docker push registry.hub.docker.com/teiid/teiid:${project.version}
fi
