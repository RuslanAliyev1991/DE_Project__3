#!/bin/bash

echo "process are started..."

docker exec --user root -it spark-master bash -c "mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars && chmod -R 777 /home/spark"
docker exec --user root -it spark-master python /opt/spark/app/stream.py
