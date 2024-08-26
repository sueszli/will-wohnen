#!/bin/bash

# Usage: ./alive.sh <python_file>

python_file="$1"

# start process
# $PWD/.venv/bin/python3 "$python_file" > run.log 2>&1 & echo $! > run.pid
python3 "$python_file" > run.log 2>&1 & echo $! > run.pid

# restart after process dies
while true; do
    if [ -f "run.pid" ] && ! ps -p $(cat "run.pid") > /dev/null 2>&1; then
        echo "process died, restarting..."

        rm -f run.pid
        rm -f run.log

        # $PWD/.venv/bin/python3 "$python_file" > run.log 2>&1 & echo $! > run.pid
        python3 "$python_file" > run.log 2>&1 & echo $! > run.pid
    fi
    sleep 5 # check every 5 seconds
done
