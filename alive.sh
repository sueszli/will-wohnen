# turn this into a makefile thing

# Usage:
# 
# $ ./alive.sh <python_file>
# 

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <python_file>"
    exit 1
fi
if [ ! -f "$1" ]; then
    echo "Error: $1 is not a file"
    exit 1
fi

python_file=$1

monitor() {
    while true; do
        if ! pgrep -f "$python_file" > /dev/null; then
            echo "$(date): process died, restarting..." >> alive-monitor.log
            rm -rf "alive.log"
            rm -rf "alive.pid"
            python3 "$python_file" >> "alive.log" 2>&1 &
            echo $! > "alive.pid"
        fi
        sleep 5
    done
}
monitor >> "alive-monitor.log" 2>&1 &
echo $! > "alive-monitor.pid"
echo "$(date): started" >> "alive-monitor.log"

# watch
while true; do clear; tail -n 100 alive.log; sleep 0.1; done
watch -n 0.1 "tail -n 100 alive.log"
pgrep -f "$python_file"
nvtop
htop

# kill
kill $(cat "alive.pid")
rm -f alive.log
rm -f alive.pid
kill $(cat "alive-monitor.pid")
rm -f alive-monitor.log
rm -f alive-monitor.pid
