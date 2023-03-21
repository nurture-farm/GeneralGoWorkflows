#!/bin/bash

git pull origin master
git fetch --force --tags

# Making a git tag for Go modules
git fetch --tags --force
#last_tag_revision=$(git rev-list --tags --max-count=1)
#tag=$(git describe --tags "$last_tag_revision")
tag=$(git tag -l --sort=-"version:refname" | head -n1)
next_tag=$tag

if [[ "$tag" =~ v([0-9]+).([0-9]+).([0-9]+)$ ]]; then
  major=${BASH_REMATCH[1]}
  minor=${BASH_REMATCH[2]}
  patch=${BASH_REMATCH[3]}
  next_patch=$((patch + 1))
  echo "old patch: $patch, new patch: $next_patch"
  next_tag="v$major.$minor.$next_patch"
fi

echo "Last tag: $tag, new tag: $next_tag"
git tag "$next_tag"
git push origin "$next_tag"
