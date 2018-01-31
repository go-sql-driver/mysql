#!/bin/sh
while :
do
    if mysql --user=gotest --password=secret --host=127.0.0.1 --port=3307 -e 'select version()' 2>&1 | grep 'version()\|ERROR 2059 (HY000):'; then
        break
    fi
    sleep 3
done
