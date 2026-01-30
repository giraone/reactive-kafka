#!/bin/bash

typeset -i nextPos=25

function openTerm {
  name=$1
  shift
  echo "$@"
  pos=$nextPos
  (( nextPos += 205 ))
  gnome-terminal --title="${name}" --profile=Small --geometry 320x10+0+${pos} -- "$@"
}

openTerm produce      bash -c "docker logs -f produce | tee produce.log"
openTerm pipe         bash -c "docker logs -f pipe    | tee pipe.log"
openTerm consume      bash -c "docker logs -f consume | tee consume.log"
