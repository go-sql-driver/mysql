#!/bin/sh

# use the mysql client inside the docker container if docker is running
[ "$(docker inspect -f '{{.State.Running}}' mysqld 2>/dev/null)" = "true" ] && mysql() {
    docker exec mysqld mysql "${@}"
}

while :
do
    if mysql --protocol=tcp -e 'select version()'; then
        break
    fi
    sleep 3
done
