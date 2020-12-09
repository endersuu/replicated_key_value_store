#!/bin/bash +vx

ip1="$(hostname -I)"
ip=${ip1::-1}

#ip="128.226.114.202"

rm -f nodes.txt

for port in {9000..9003}; do
  echo "$ip":$port >>nodes.txt
done

for port in {9000..9003}; do
  python3 src/server.py $port &
  sleep 1
done