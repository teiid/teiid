#!/bin/bash
set -e

sudo docker build --rm -t teiid/teiid:${project.version} .

echo "Want to push the image to Docker Hub (y/n) ?"

read selection

if [ "$selection" == "y" ]; then
    sudo docker login
    sudo docker push teiid/teiid:${project.version}
fi
