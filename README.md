# Kub
Work in progress kubernetes native build system. 

# Running
Get dependencies:

```
$ pip install -r requirements.txt
```

Run the tests:

```
$ python -m unittest discover tests
```

First make sure `~/.kube/config` points at a live cluster. To launch rabbitmq:

```
$ docker-compose up -d
```
To launch the server:

```
$ python server.py
```

The rabbit management interface is accessible at 15672 on localhost, from there you can send a test message.
Example test message:

```json
{"name": "test", "command": ["/bin/sh", "-c"], "image": "alpine", "args": ["sleep 10 && exit 1"]}
```

# Warning! 
This is a work in progress, do not test on a production cluster!