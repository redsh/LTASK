
Tasks-In-A-Bottle
=========

Minimal distributed task queue, written in Python. 

Workers get tasks from a server using HTTP requests.

A task is identified by a set of parameters.

The server only provides task parameters, sending them as messages in a bottle.


====
Task Server
====

The server, located at server_address is used by 

```sh
server$ python ltask/webtask.py --server -p examples.dummy.scan 
```

In the python module "examples.dummy" two entities are defined:

- The class Task, representing a task instance and the command/code it has to run (member function run()). A Task is constructed using a set of parameters.

- One or many parameter sets, in this case provided the function 'scan()'. These parameter sets are communicated to the workers and used by them to construct Tasks.

- Tasks output directories are named after the hash of their parameter set.

====
Workers
====

A worker is spawned:

```sh
worker-1$ python ltask/webtask.py http://server_address:1988
```

You can specify the number of concurent task that the current worker node can handle with -x:

```sh
worker-1$ python ltask/webtask.py http://server_address:1988 -x 12
```

You can setup ssh tunnel on local port 19880 with the -t flag:

```sh
worker-2$ python ltask/webtask.py server_address -t
```

(-t is equivalent to: ssh -NfL 19880:localhost:1988 server_address)

=====
Gather/reduce output
=====

So far, we've been using the system writing in the same network filesystem, but a system for gathering the output from the different machines where the tasks are being run is under development.

=====
Submitting work: Enter Controllers
=====

To submit tasks use --post:

```sh
worker-1$ python ltask/webtask.py http://server_address:1988 -p examples.dummy.scan  --post 
```
(tasks are defined defined in examples.dummy.scan).

By default, workers exit when the server task queue is empty. To keep them alive and looking for new work, use: 

```sh
worker-1$ python ltask/webtask.py localhost -t --retry_if_empty
```

One could spawn the server, launch '--retry_if_empty' workers and submit work using post without touching the worker and server machines.

=========
Installing & Requirements
=========
```sh
pip install -r pip_requirements_optional.txt
```
