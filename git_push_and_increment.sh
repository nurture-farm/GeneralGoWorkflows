#!/bin/bash

# Git upload
git add .
if [[ -z "${1//  }" ]]
then
  git commit
else
  git commit -m "$1"
fi
git pull origin master || exit
git push origin master || exit

sh ./git_increment_version.sh