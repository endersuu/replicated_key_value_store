# replicated_key_value_store

### Initialize project
```text
dfan2@remote02:~/replicated_key_value_store$ ls
chord.thrift  gen-py  README.md  server.sh  src  STATEMENT  terminate_servers.py
dfan2@remote02:~/replicated_key_value_store$ chmod 700 server.sh
```

### How to initialize servers
```text
Before running server, make sure run "python3 terminate_servers.py" first to clean up environment
dfan2@remote02:~/replicated_key_value_store$ python3 terminate_servers.py
    PID TTY          TIME CMD
1510008 pts/0    00:00:00 python3
1510011 pts/0    00:00:00 sh
1510012 pts/0    00:00:00 ps
dfan2@remote02:~/replicated_key_value_store$ ./server.sh
server.py    |<module>     |367|Starting the server... pid = 1206069, port = 9000
server.py    |<module>     |367|Starting the server... pid = 1206072, port = 9001
server.py    |<module>     |367|Starting the server... pid = 1206079, port = 9002
server.py    |<module>     |367|Starting the server... pid = 1206086, port = 9003
```

### Crash and recover server
```text
Crash and recover server 0
dfan2@remote02:~/replicated_key_value_store$ kill 1206069
dfan2@remote02:~/replicated_key_value_store$ python3 src/server.py 9000 &

Crash and recover server 1
dfan2@remote02:~/replicated_key_value_store$ kill 1206072
dfan2@remote02:~/replicated_key_value_store$ python3 src/server.py 9001 &
```

### Send command to coordinator by console
```text
dfan2@remote02:~/replicated_key_value_store$ python3 src/client.py
Wait for commands:
put 0 value0 one
client.py    |__enter__    |72 |Client has connected to 128.226.114.202:9002
client.py    |main         |103|ServerResponse(status='SUCCESS', kv=None)
Wait for commands:
put 137 value137 quorum
client.py    |__enter__    |72 |Client has connected to 128.226.114.202:9002
client.py    |main         |103|ServerResponse(status='SUCCESS', kv=None)
Wait for commands:
get 137 quorum
client.py    |__enter__    |72 |Client has connected to 128.226.114.202:9002
client.py    |main         |103|ServerResponse(status='SUCCESS', kv=KV(key=137, value='value137', time=160670252899928))
Wait for commands:
exit
dfan2@remote00:~/replicated_key_value_store$
```


### Inspect servers
```text
dfan2@remote02:~/replicated_key_value_store$ python3 src/inspector.py
{'serverID': 0, 'kvMem': {0: KV(key=0, value='value0', time=160670249285699), 137: KV(key=137, value='value137', time=160670252899928)}}
{'serverID': 1, 'kvMem': {0: KV(key=0, value='value0', time=160670249285699)}}
{'serverID': 2, 'kvMem': {0: KV(key=0, value='value0', time=160670249285699), 137: KV(key=137, value='value137', time=160670252899928)}}
{'serverID': 3, 'kvMem': {137: KV(key=137, value='value137', time=160670252899928)}}
```

### python version
```text
dfan2@remote02:~/replicated_key_value_store$ python3 --version
Python 3.7.3
```


### Contributions
Dali Fan: Framework, Review, Test, Documents
Yongheng Li: Core logic, Function definition, Persistent storage


### Completion status
Finished all the features required in this assignment, also the test.  
Include features:
 - integrated coordinators and replicas: src/server.py
 - persistent log database which makes the system resilient for a crash: src/database.py
 - client work as a console: src/client.py
 - inspector help to check the inner states of replicas: src/inspector.py
 - scripts for initializing and terminate project: server.sh/terminate_servers.py

Tested features:  
 - data stored duplicately into 3 assigned replicas decided by ByteOrderedPartitioner, and update successfully
 - quorum reading, coordinator compares and returns the most recent data
 - quorum writing, coordinator discards the request and returns an exception if less than 2 replicas are alive
 - one reading, coordinator discards the request and returns an exception if less than 1 replicas are alive
 - replica crashes and recovers by its persistent log
 - replica crashes and recovers by hints handoff
 