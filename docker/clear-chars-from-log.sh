#!/bin/bash

for file in "$@"; do
  sed $'s/\033\[[0-9;]*m//g' < "$file" > "$file.bak" && mv "$file.bak" "$file"
done