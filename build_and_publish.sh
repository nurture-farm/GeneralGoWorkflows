#!/bin/bash

export DOCKER_BUILDKIT=0

build_out=$(sh build.sh | tee /dev/fd/2 | grep "Successfully built ")
if [[ -z "$build_out" ]]
then
  echo "Exiting since build failed"
  exit 1
fi


final_tag="latest"
if [[ -z "$1" ]]
then
  echo "Building with $final_tag tag"
else
  final_tag=$1
  echo "Using $1 as tag name for ECR build"
fi

parsed_tag=""
if [[ $build_out =~ Successfully[[:space:]]built[[:space:]]([a-z0-9]+)$ ]]
then
  parsed_tag=${BASH_REMATCH[1]}
fi
echo "Parsed tag is $parsed_tag"