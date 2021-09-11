# Faktory Client Python ~~faktory_worker_python~~

🚧 **Work In Progress..**

🔴 **Don't use in production**

⚠️ **Will be ready in the very next few days**

This repository provides Python Client (Consumer and Producer) for the [Faktory](https://github.com/contribsys/faktory/) background job server.

```
                   +--------------------+
                   |                    |
                   |     Faktory        |
                   |     Server         |
          +---->>>>|                    +>>>>----+
          |        |                    |        |
          |        |                    |        |
          |        +--------------------+        |
          |                                      |
          |                                      |
          |                                      |
          |                                      |
+-----------------------------------------------------------+     
|         .                Faktory               .          |            
|         .                Client                .          |  
|         .                                      .          |          
|   +-----------------+               +-----------------+   |
|   |                 |               |                 |   |
|   |    Producer     |               |    Consumer     |   |
|   |     pushes      |               |    (Worker)     |   |
|   |      jobs       |               |     fetches     |   |
|   |                 |               |       jobs      |   |
|   |                 |               |                 |   |
|   +-----------------+               +-----------------+   |          
|                                                           |            
+-----------------------------------------------------------+            
```

- [Server](https://github.com/contribsys/faktory/) - the Faktory daemon which stores background jobs in queues to be processed by Workers.
- Client - an entity that communicates with the Faktory server using the [FWP](https://github.com/contribsys/faktory/blob/master/docs/protocol-specification.md). A single client can act as both a consumer and a producer.
- Consumer (or Worker) - a client that fetches work units (jobs) from the server for execution. 
- Producer - a client that issues new work units to the server. 

This library tries to implement the [FWP](https://github.com/contribsys/faktory/blob/master/docs/protocol-specification.md) as well as possible. If you notice any inconsistencies, please report them.

## Installation

```
pip install pyfaktory
```

## Usage

### Faktory Server

If you have a Faktory server already running, make sure you have the correct url.

```python
# Default url for a Faktory server running locally
faktory_server_url = 'tcp://localhost:7419'
```

For the installation of faktory, please refer to [the official documentation](https://github.com/contribsys/faktory/wiki/Installation).

After installation, you can run it locally.

```console
$ /usr/bin/faktory 
Faktory 1.5.2
Copyright © 2021 Contributed Systems LLC
Licensed under the GNU Affero Public License 3.0
I 2021-08-30T06:58:37.050Z Initializing redis storage at /home/albatros/.faktory/db, socket /home/albatros/.faktory/db/redis.sock
I 2021-08-30T06:58:37.061Z Web server now listening at localhost:7420
I 2021-08-30T06:58:37.061Z PID 23549 listening at localhost:7419, press Ctrl-C to stop
```

You can use a password for the Faktory server via the environment variable `FAKTORY_PASSWORD`. Note if this value starts with a `/`, then it is considered a pointer to a file on the filesystem with the password. By default `/etc/faktory/password` is used.

The format of the Faktory URL is as follows:
```
tcp://:password@localhost:7419
```

To run Faktory in production:
```
/usr/bin/faktory -e production
```

Faktory in production mode requires a password by default since version 0.7.0.

### Faktory Client

Import `pyfaktory`.

```python
import pyfaktory
```

A single client can act as both a consumer and a producer.

```python
client = pyfaktory.Client(faktory_url='tcp://localhost:7419')
client.connect()
# Now you can use the client
# At the end, diconnect the client
client.disconnect()
```

Use `role` argument to say how you want to use the client.

### Producer

Use the client to push jobs.

```python
producer = pyfaktory.Producer()
```

### Consumer (Worker)

Use a worker to pull jobs from Faktory server and execute them.

```python
consumer = pyfaktory.Consumer()
consumer.run()
```

## Example

Find examples in `./examples`.

- Start the Faktory server.
```
/usr/bin/faktory
```

- Launch a producer.
```
python examples/fproducer.py
```

- Launch a consumer
```
python examples/fconsumer.py
```

- Look at what is happening in the logs and in the [Faktory Web UI](http://localhost:7420/)

## Contribute

### Issues

If you encounter a problem, please report it.

In addition to the description of your problem, report the server and client 
versions.

```
pyfaktory.__version__ 
```
```
/usr/bin/faktory -v
```

Reproduce your problem while increasing the level of debugging for both the 
server and the client, and report the logs.
```
import logging
logging.basicConfig(level=logging.DEBUG)
```
```
/usr/bin/faktory -l debug
```

### PRs

Please, feel free to make PRs.
