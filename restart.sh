#!/bin/bash
pkill -f "uvicorn app.main:app"
sleep 1
cd /home/irst/gliotwin
nohup uvicorn app.main:app --host 0.0.0.0 --port 8000 >> /home/irst/gliotwin/uvicorn.log 2>&1 &
echo "Server avviato (pid $!)"
sleep 1
tail -3 /home/irst/gliotwin/uvicorn.log
