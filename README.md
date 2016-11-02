Clique Connector
================

Library that wraps a RabbitMQ connection and creates a channel between Clique agents and APIs.

Install
-------

`curd install git+git@bitbucket.org:bagis/clique-connector.git`

Features
--------

* Send statistics
* Listen for statistics
* Ask for a virtual machine
* Confirm a virtual machine job
* Confirm with a virtual machine

How to test
-----------

* Setup a RabbitMQ, preferably a docker container: `docker run --name rabbitmq -p 25672:25672 -p 4369:4369 -p 5671-5672:5671-5672 rabbitmq`
Then run all the tests: `python -m unittest test/**/*.py`

How to use
----------

```python
from clique_connector import Connector

connector = Connector('127.0.0.1')

# Request a virtual machine
machine = await connector.create_machine(
    'some-random-machine', # Machine name
    'ubuntu-16.04', # Image name
    1, # CPU
    512, # Memory in MB
    128, # Disc in GB
    'your-public-ssh-key-as-string') # Public SSH key

# Response with a virtual machine
def callback(channel_close, name, cpu, mem, disc, pkey):
    return dict(host='127.0.0.1',
                username='root')

# Create listener
channel_close, observable = connector.wait_for_machines(callback)

# Start listening
machines = await observable \
    .take(3) \ # Create only three machines
    .last(lambda _: channel_close())
```
