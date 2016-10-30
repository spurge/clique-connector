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

    @property
    def channel(self):
        if self.__channel is None:
            self.__channel = self.connection.channel()

        return self.__channel

    @property
    def command_queue(self):
        self.channel.queue_declare(queue=self.COMMAND_QUEUE_NAME)
        return self.COMMAND_QUEUE_NAME

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

    def response_queue(self, uuid, checksum):
        queue = self.RESPONSE_QUEUE_NAME % (uuid, checksum)

        if self.uuid == uuid:
            self.channel.queue_declare(
                queue=queue,
                exclusive=True)
        else:
            self.channel.queue_declare(queue=queue)

        return queue

    def encode_message(self, **kwargs):
        props = dict(uuid=self.uuid,
                     time=time(),
                     **kwargs)
        checksum = md5(json.dumps(props).encode('utf8')).hexdigest()
        props['checksum'] = checksum
        return checksum, json.dumps(props)

    def publish_online(self):
        return self.publish_stats(uname=uname())

    def publish_command(self, command, **kwargs):
        checksum, message = self.encode_message(command=command, **kwargs)
        self.channel.basic_publish(exchange='',
                                   routing_key=self.command_queue,
                                   body=message)
        return checksum

    def publish_response(self, uuid, response_checksum, **kwargs):
        checksum, message = self.encode_message(**kwargs)
        self.channel.basic_publish(exchange='',
                                   routing_key=self.response_queue(
                                       uuid, response_checksum),
                                   body=message)
        return checksum

    def publish_stats(self, **stats):
        logging.debug('Publish stats', extra=stats)
        channel = self.connection.channel()
        checksum, body = self.encode_message(**stats)
        message = Message.create(channel,
                                 body,
                                 {'content_type': 'application/json'})
        message.publish(**self.status_exchange(channel))
        channel.close()
        return checksum

    def decode_message(self, callback, message):
        logging.debug('Got message: %s', message.body)

        try:
            callback(message)
        except Exception as error:
            logging.error('Error decoding message', extra=error)
            message.nack()

    def create_listener(self, channel, kwargs, observer):
        logging.debug('Creates listener', extra=kwargs)

        try:
            channel.basic.qos(1)
            channel.basic.consume(partial(self.decode_message,
                                          observer.on_next),
                                  **kwargs)
            channel.start_consuming()
        except Exception as error:
            channel.stop_consuming()
            observer.on_error(error)

        logging.debug('Listener completed', extra=kwargs)
        channel.close()
        observer.on_completed()

    def get_status_listener(self):
        logging.debug('Listens to status')
        channel = self.connection.channel()
        queue = self.status_queue(channel)
        observable = Observable.create(partial(self.create_listener,
                                               channel,
                                               queue)) \
            .where(lambda m: m is not None) \
            .tap(lambda m: m.ack())

        return channel.stop_consuming, observable

    def get_command_listener(self):
        return Observable.create(partial(self.create_listener,
                                         dict(queue=self.command_queue))) \
            .where(lambda m: m is not None)

    def get_response_listener(self, checksum):
        return Observable.create(partial(self.create_listener,
                                         dict(queue=self.response_queue(
                                            self.uuid, checksum)))) \
            .where(lambda m: m is not None)

    def stop_listeners(self):
        self.channel.stop_consuming()
