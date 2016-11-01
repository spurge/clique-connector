# -*- coding: utf-8 -*-

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
        if self.__uuid is None:
            self.__uuid = str(uuid1())

        return self.__uuid

    @property
    def connection(self):
        if self.__connection is None:
            logging.debug('Connect to AMQP: %s', self.host)
            self.__connection = Connection(self.host, 'guest', 'guest')

        return self.__connection

    def command_queue(self, channel):
        channel.queue.declare(self.COMMAND_QUEUE_NAME)
        return dict(routing_key=self.COMMAND_QUEUE_NAME)

    def status_exchange(self, channel):
        logging.debug('Declaring status exchange: %s',
                      self.STATUS_EXCHANGE_NAME)
        channel.exchange.declare(exchange=self.STATUS_EXCHANGE_NAME,
                                 exchange_type='fanout')
        return dict(exchange=self.STATUS_EXCHANGE_NAME,
                    routing_key='')

    def status_queue(self, channel):
        name = self.STATUS_QUEUE_NAME % self.uuid
        logging.debug('Declaring status queue: %s', name)
        channel.queue.declare(name,
                              exclusive=True)
        channel.queue.bind(queue=name,
                           **(self.status_exchange(channel)))
        return dict(queue=name)

    def response_queue(self, uuid, checksum, channel):
        name = self.RESPONSE_QUEUE_NAME % (uuid, checksum)
        logging.debug('Declaring response queue: %s', name)

        channel.queue.declare(name)

        return dict(routing_key=name)

    def encode_message(self, **kwargs):
        props = dict(uuid=self.uuid,
                     time=time(),
                     **kwargs)
        checksum = md5(json.dumps(props).encode('utf8')).hexdigest()
        props['checksum'] = checksum
        return checksum, json.dumps(props)

    def publish(self, contents, publish_args_generator):
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
        return self.publish_stats(uname=uname())

    def publish_command(self, command, **kwargs):
        return self.publish(dict(command=command, **kwargs),
                            self.command_queue)

    def publish_response(self, uuid, checksum, **kwargs):
        return self.publish(kwargs, partial(self.response_queue,
                                            uuid,
                                            checksum))

    def publish_stats(self, **stats):
        return self.publish(stats, self.status_exchange)

    def decode_message(self, callback, message):
        logging.debug('Got message: %s', message.body)

        try:
            callback(message)
        except Exception as error:
            logging.error('Error decoding message', extra=error)
            message.nack()

    def get_listener(self, listen_args_generator):
        channel = self.connection.channel()
        queue = listen_args_generator(channel)

        channel.basic.qos(1)

        if 'routing_key' in queue:
            queue = dict(queue=queue['routing_key'])

        observable = Observable.interval(100,
                                         scheduler=AsyncIOScheduler()) \
            .map(lambda _: channel.basic.get(**queue)) \
            .where(lambda m: m is not None) \
            .tap(lambda m: logging.debug('Got message %s', m.body))

        return channel.close, observable

    def get_status_listener(self):
        logging.debug('Listens to status')
        return self.get_listener(self.status_queue)

    def get_command_listener(self):
        logging.debug('Listens to commands')
        return self.get_listener(self.command_queue)

    def get_response_listener(self, checksum):
        logging.debug('Listens to responses for %s', checksum)
        return self.get_listener(partial(self.response_queue,
                                         self.uuid,
                                         checksum))
