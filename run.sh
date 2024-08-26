python3 -m venv .venv && source .venv/bin/activate

pip install -r requirements.txt

# stay alive
python_file="./0-mining/scrape_pages.py"
# python_file="./XYZ.py"
chmod +x run-monitor.sh
nohup ./run-monitor.sh "$python_file" > run-monitor.log 2>&1 & echo $! > "run-monitor.pid"

# run once
nohup python3 "./src/3-eval_cls_robustified_model.py" > run.log 2>&1 & echo $! > "run.pid"

# monitor
watch -n 0.1 "tail -n 100 run.log"
pgrep -f "$python_file"

# kill
kill $(cat "run-monitor.pid")
rm -f run-monitor.pid
rm -f run-monitor.log
kill $(cat "run.pid")
rm -f run.log
rm -f run.pid
