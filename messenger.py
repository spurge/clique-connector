# -*- coding: utf-8 -*-

"""
Copyright (c) 2016 Olof Montin <olof@montin.net>

This file is part of clique-connector.
"""

import json
import logging

from amqpstorm import Connection, \
                      Message
from functools import partial
from hashlib import md5
from os import uname
from rx import Observable
from rx.concurrency import AsyncIOScheduler
from time import time
from uuid import uuid1


class Messenger:
    """Wraps a RabbitMQ connection through the amqpstorm library
    (https://github.com/eandersson/amqpstorm) and creates command,
    status and unique response queues for both publishing and listening.

    The command queue is like a round-robin task/job queue. Used for
    sending tasks to agents.

    The status queue is rather an exchange with type fanout, which goes
    out to all listeners. It's used for logging statistics.

    All listeners uses Reactive X Observables (http://reactivex.io).

    Use like so:
        messenger = Messenger('127.0.0.1')

        # Publish a command
        checksum_of_command = messenger.publish_command('some-command',
                                                        key='value')

        # Listen for commands
        channel_close, observable = messenger.get_command_listener()

        # Get first command and close channel
        command = await observable \
                        .first() \
                        .tap(lambda _: channel_close())

        # All messanges will contain the following minimum:
        print('The messengers uuid: %s' % command['uuid'])
        print('Message checksum: %s' % command['checksum'])
        print('When message was sent: %d' % command['time'])

        # Send response
        checksum_of_response = messenger.publish_response(
            command['uuid'],
            command['checksum'])

        # Listen for responses
        channel_close, observable = messenger.get_response_listener(
            checksum_of_command)

        # Get first response and close channel
        response = await observable \
                         .first() \
                         .tap(lambda _: channel_close())
    """

    LISTENER_INTERVAL = 100
    COMMAND_QUEUE_NAME = 'clique-command'
    RESPONSE_QUEUE_NAME = 'clique-response-%s-%s'
    STATUS_EXCHANGE_NAME = 'clique-status'
    STATUS_QUEUE_NAME = 'clique-status-%s'

    def __init__(self, host):
        self.host = host
        self.__uuid = None
        self.__connection = None
        self.__channel = None

        self.publish_online()

    @property
    def uuid(self):
        """Sets and returns a uuid based on the host mac address.
        """

        if self.__uuid is None:
            self.__uuid = str(uuid1())

        return self.__uuid

    @property
    def connection(self):
        """Sets and returns a RabbitMQ connection.
        """

        if self.__connection is None:
            logging.debug('Connect to AMQP: %s', self.host)
            self.__connection = Connection(self.host, 'guest', 'guest')

        return self.__connection

    def command_queue(self, channel):
        """Declares a command queue in provided channel.
        Returns a dict with a routing_key.
        Can be used by both publishing and listening.
        """

        channel.queue.declare(self.COMMAND_QUEUE_NAME)

        return dict(routing_key=self.COMMAND_QUEUE_NAME)

    def status_exchange(self, channel):
        """Declares a status exchange in provided channel.
        Returns a dict with exchange and routing_key.
        Can only be used when publishing.
        """

        logging.debug('Declaring status exchange: %s',
                      self.STATUS_EXCHANGE_NAME)

        channel.exchange.declare(exchange=self.STATUS_EXCHANGE_NAME,
                                 exchange_type='fanout')

        return dict(exchange=self.STATUS_EXCHANGE_NAME,
                    routing_key='')

    def status_queue(self, channel):
        """Declaring a unique status queue binded to the status
        exchange in provided channel.
        Returns a dict with queue.
        Can only be used for listening.
        """

        name = self.STATUS_QUEUE_NAME % self.uuid
        logging.debug('Declaring status queue: %s', name)

        channel.queue.declare(name,
                              exclusive=True)
        channel.queue.bind(queue=name,
                           **(self.status_exchange(channel)))

        return dict(queue=name)

    def response_queue(self, uuid, checksum, channel):
        """Declares a unique response queue in provided channel based
        on uuid and checksum.
        Returns a dict with routing_key.
        Can be used by both publishing and listening.
        """

        name = self.RESPONSE_QUEUE_NAME % (uuid, checksum)
        logging.debug('Declaring response queue: %s', name)

        channel.queue.declare(name)

        return dict(routing_key=name)

    def encode_message(self, **kwargs):
        """Adds the messenger's uuid, time and the body's checksum
        to the message and encodes it to json.
        The message's body is set by keywork arguments.
        Returns the message checksum and the message body as a json
        string.
        """

        props = dict(uuid=self.uuid,
                     time=time(),
                     **kwargs)
        checksum = md5(json.dumps(props).encode('utf8')).hexdigest()
        props['checksum'] = checksum

        return checksum, json.dumps(props)

    def publish(self, contents, publish_args_generator):
        """Publish a message to the RabbitMQ connection with a
        temporary channel created for just this task.
        Message contents provided as a dict and a publish message
        argument generator as a function callback.
        Returns a checksum of the message body.
        """
        channel = self.connection.channel()
        checksum, body = self.encode_message(**contents)

        logging.debug('Publish message: %s', body)

        message = Message.create(channel,
                                 body,
                                 {'content_type': 'application/json'})
        message.publish(**publish_args_generator(channel))

        channel.close()

        return checksum

    def publish_online(self):
        """Publish a statistics message about this messenger in the
        statistics exchange.
        Returns the message's checksum
        """

        return self.publish_stats(uname=uname())

    def publish_command(self, command, **kwargs):
        """Publish a command to the command queue and returns the
        message's checksum.
        """

        return self.publish(dict(command=command, **kwargs),
                            self.command_queue)

    def publish_response(self, uuid, checksum, **kwargs):
        """Publish a response by uuid and checksum to the unique
        response queue and returns with the message's checksum.
        """

        return self.publish(kwargs, partial(self.response_queue,
                                            uuid,
                                            checksum))

    def publish_stats(self, **stats):
        """Publish statistics in the statistics exchange and returns
        the message's checksum.
        """

        return self.publish(stats, self.status_exchange)

    def get_listener(self, listen_args_generator):
        """Get a listener as an observable that fetches messages
        by interval by provided queue argument generator callback.
        Creates a new channel for this purpose.
        Returns the channel's close function and the observable.
        """

        channel = self.connection.channel()
        queue = listen_args_generator(channel)

        # Take one message at a time. This setting will force the client
        # to acknoledge the message before it will be able to recieve
        # another one.
        channel.basic.qos(1)

        if 'routing_key' in queue:
            queue = dict(queue=queue['routing_key'])

        # Creates a non-blocking interval based asyncio observable.
        # It has to be an interval for the non-blocking purpose.
        # The asyncio scheduler is necessary for the awaitables.
        observable = Observable.interval(self.LISTENER_INTERVAL,
                                         scheduler=AsyncIOScheduler()) \
            .map(lambda _: channel.basic.get(**queue)) \
            .where(lambda m: m is not None) \
            .tap(lambda m: logging.debug('Got message %s', m.body))

        return channel.close, observable

    def get_status_listener(self):
        """Gets a status exchange listener and returns the channel's
        close function and an observable.
        """

        logging.debug('Listens to status')
        return self.get_listener(self.status_queue)

    def get_command_listener(self):
        """Gets a command queue listener and returns the channel's
        close function and an observable.
        """

        logging.debug('Listens to commands')
        return self.get_listener(self.command_queue)

    def get_response_listener(self, checksum):
        """Gets a response queue listener based on this messenger's uuid
        and provided checksum.
        Returns the channel's close function and an observable.
        """

        logging.debug('Listens to responses for %s', checksum)
        return self.get_listener(partial(self.response_queue,
                                         self.uuid,
                                         checksum))
