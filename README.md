# faktory_worker_python

**Work in progress...**

This repository provides Python client and worker process for the [Faktory](https://contribsys.com/faktory/) background job server.

```
                       +--------------------+
                       |                    |
                       |     Faktory        |
                       |     Server         |
        +---------->>>>|                    +>>>>--------+
        |              |                    |            |
        |              |                    |            |
        |              +--------------------+            |
+-----------------+                            +-------------------+
|                 |                            |                   |
|    Client       |                            |     Worker        |
|    pushes       |                            |     fetches       |
|     jobs        |                            |      jobs         |
|                 |                            |                   |
|                 |                            |                   |
+-----------------+                            +-------------------+
```

- Client - an API any process can use to push jobs to the Faktory server.
- Worker - a process that pulls jobs from Faktory and executes them.
- Server - the Faktory daemon which stores background jobs in queues to be processed by Workers.

This library contains only the client and worker parts. The server part is [here](https://github.com/contribsys/faktory/).
