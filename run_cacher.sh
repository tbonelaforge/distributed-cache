#!/bin/bash

# Accessing arguments
script_name="$0"
id_argument="$1"

target_port=$((5000 +  $id_argument))

COMMAND="flask --app 'cacher:create_app($id_argument, \"http://localhost:5000\")' run --port $target_port"
echo "The flask command is: "
echo $COMMAND

eval "$COMMAND"
