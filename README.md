# Faktory Client Python (faktory_worker_python)

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
Faktory 1.6.0
```

You can use a password for the Faktory server via the environment variable `FAKTORY_PASSWORD`. Note if this value starts with a `/`, then it is considered a pointer to a file on the filesystem with the password. By default `/etc/faktory/password` is used.

The format of the Faktory URL is as follows:
```
tcp://:password@localhost:7419
```

You can access the [Fakotry GUI](http://localhost:7420/).

To run Faktory in production:
```
/usr/bin/faktory -e production
```

Faktory in production mode requires a password by default since version 0.7.0.

### Faktory Client

Import `pyfaktory`.

```python
from pyfaktory import Client, Consumer, Job, Producer
```

A single client can act as both a consumer and a producer.

```python
client = Client(faktory_url='tcp://localhost:7419')
client.connect()

# Now you can use the client

# At the end, disconnect the client
client.disconnect()
```

Client is a context manager, so you can use `with` statement.

```python
with Client(faktory_url='tcp://localhost:7419') as client:
    # Use client
```

Use `role` argument to say how you want to use the client. This argument has 
three possible values: 'consumer', 'producer' or 'both'.

```python
# A client that acts as both a consumer and a producer.
client = Client(faktory_url='tcp://localhost:7419', role='both')
```

### Producer

Use the client to push jobs.

#### Push job

```python
with Client(faktory_url='tcp://localhost:7419', role='producer') as client:
    producer = Producer(client=client)
    job_1 = Job(jobtype='adder', args=(5, 4), queue='default')
    producer.push(job_1)
```

#### Push bulk jobs

You can push several jobs at once. There is no limit, but 1000 at a time is recommended as a best practice.

```python
with Client(faktory_url='tcp://localhost:7419', role='producer') as client:
    producer = Producer(client=client)
    job_2 = Job(jobtype='adder', args=(50, 41))
    job_3 = Job(jobtype='adder', args=(15, 68))
    res = producer.push_bulk([job_2, job_3])
```

### Consumer (Worker)

Use a worker to pull jobs from Faktory server and execute them.

```python
def adder(x, y):
    logging.info(f"{x} + {y} = {x + y}")

with Client(faktory_url='tcp://localhost:7419', role='consumer') as client:
    consumer = Consumer(client=client, queues=['default'], concurrency=1)
    consumer.register('adder', adder)
    consumer.run()
```

Use `priority` to indicates in which queue order the jobs should be fetched 
first.

```python
# With strict priority, there is a risk of starvation
consumer = Consumer(client=client, queues=['critical', 'default', 'bulk'], priority='strict')
# Each queue has an equal chance of being fetched first
consumer = Consumer(client=client, queues=['critical', 'default', 'bulk'], priority='uniform')
# Weights must be specified
consumer = Consumer(client=client, queues=['critical', 'default', 'bulk'], priority='weighted', weights=[0.6, 0.3, 0.1])
```

### Capture exceptions using Sentry

To capture exceptions using Sentry before failling jobs 
set `sentry_capture_exception` argument to `True`.

```python
consumer = Consumer(client=client, sentry_capture_exception=True)
```

### Info

You can get various information about the server using `info` Client method.

```python
with Client(faktory_url='tcp://localhost:7419') as client:
    server_info = client.info()
    print(server_info)
```

### Mutate

A wrapper for the [Mutate API](https://github.com/contribsys/faktory/wiki/Mutate-API) to script certain repairs or migrations.

⚠️ MUTATE commands can be slow and/or resource intensive. They should not be used as part of your application logic.

```python
from pyfaktory import Client, JobFilter, MutateOperation

client = Client(faktory_url='tcp://localhost:7419')
client.connect()

# Find all scheduled jobs with type `QuickbooksSyncJob` and discard them
op = MutateOperation(
    cmd='discard', 
    target='scheduled', 
    filter=JobFilter(jobtype="QuickbooksSyncJob")
)
client.mutate(op)

# Clear the Retry queue completely
op = MutateOperation(
    cmd='discard', 
    target='retries', 
    filter=JobFilter(regexp="*")
)
client.mutate(op)

# Clear the Retry queue completely
op = MutateOperation(
    cmd='discard', 
    target='retries'
)
client.mutate(op)

# Send a two specific JIDs in the Retry queue to the Dead queue
op = MutateOperation(
    cmd='kill', 
    target='retries', 
    filter=JobFilter(jobtype="QuickbooksSyncJob", jids=["123456789", "abcdefgh"])
)
client.mutate(op)

# Enqueue all retries with a first argument matching "bob"
op = MutateOperation(
    cmd='requeue', 
    target='retries', 
    filter=JobFilter(regexp="*\"args\":[\"bob\"*")
)
client.mutate(op)

client.disconnect()
```

### Batch (untested)

Refer to [documentation](https://github.com/contribsys/faktory/wiki/Ent-Batches).

```python
from pyfaktory import Batch, Client, Job, Producer, TargetJob

client = Client(faktory_url='tcp://localhost:7419')
client.connect()

producer = Producer(client=client)

batch = Batch(
    description="An optional description for the Web UI",
    success=TargetJob(jobtype="MySuccessCallback", args=[123], queue="critical"),
    complete=TargetJob(jobtype="MyCompleteCallback", args=['aa'], queue="critical")
)

# Create a new batch
resp = producer.batch_new(batch)

# Push as many jobs as necessary for the batch
# You may nest batches
# The initial batch data has a TTL of 30 minutes and will expire if batch is not commited
producer.push(Job(jobtype='SomeJob', args=(5, 4), custom={"bid": bid}))
producer.push(Job(jobtype='SomeOtherJob', args=(0, 15), custom={"bid": bid}))

# Commit the batch
producer.batch_commit(bid)

client.disconnect()
```

Use `batch_open` to open a created batch.

```python
producer.batch_open(bid)
```

Use `parent_bid` for child batch.

```python
child_batch = Batch(
    parent_bid=bid,
    description="An optional description for the Web UI",
    success=TargetJob(jobtype="MySuccessCallback", args=[123], queue="critical"),
    complete=TargetJob(jobtype="MyCompleteCallback", args=['aa'], queue="critical")
)
```

### Custom command

If you want to use a Faktory command that is not yet implemented in this client library, you can send custom commands.

```python
from pyfaktory import Client

my_command = 'INFO\r\n'

with Client(faktory_url='tcp://localhost:7419') as client:
    resp = client._send_and_receive(my_command)
    print(resp)
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

- Launch a consumer.
```
python examples/fconsumer.py
```

- Look at what is happening in the logs and in the [Faktory Web UI](http://localhost:7420/).

## Contribute

### Issues

If you encounter a problem, please report it.

In addition to the description of your problem, report the server and client
versions.

```python
pyfaktory.__version__
```
```
/usr/bin/faktory -v
```

Reproduce your problem while increasing the level of debugging for both the
server and the client, and report the logs.
```python
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S')
```
```
/usr/bin/faktory -l debug
```

### PRs

Please, feel free to create pull requests.