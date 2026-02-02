#!/bin/bash

docker images | awk '$1 == "<none>" { print $3}' | xargs docker rmi
