#! /bin/bash
echo "hello"
pid=$(ps aux | grep IoTEvents | awk '{print $2}')
kill -9 $pid
# or
# for pid in $(ps aux | pgrep IoTGateway); do kill -9 $pid; done
