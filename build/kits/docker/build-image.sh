#!/bin/sh

sudo docker build -rm -t teiid/teiid:${project.version} .

echo "Want to push the image to Docker Hub (y/n) ?"

read selection

if [ $selection eq 'y']; then
    sudo docker login
    sudo docker push teiid/teiid:${project.version}
fi
