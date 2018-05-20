#!/bin/sh
set -v
while :
do
    if mysql -e 'select version()' 2>&1 | grep 'ERROR 1045 (28000):\|ERROR 2059 (HY000):'; then
        break
    fi
    sleep 3
done
