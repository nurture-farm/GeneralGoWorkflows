#!/bin/bash

#go get github.com/nurture-farm/Contracts
go mod tidy
go mod vendor

# Build docker image
docker build -t general-go-workflows .
