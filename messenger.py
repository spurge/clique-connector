# -*- coding: utf-8 -*-

import json
import pika

from functools import partial
from hashlib import md5
from os import uname
from rx import Observable
from time import time
from uuid import uuid1


class Messenger:

    COMMAND_QUEUE_NAME = 'clique-command'
    STATUS_EXCHANGE_NAME = 'clique-status'
    STATUS_QUEUE_NAME = 'clique-status-%s'

    def __init__(self, url):
        self.url = url
        self.__uuid = None
        self.__connection = None
        self.__channel = None

    @property
    def uuid(self):
        if self.__uuid is None:
            self.__uuid = str(uuid1())

        return self.__uuid

    @property
    def connection(self):
        if self.__connection is None:
            parameters = pika.URLParameters(self.url)
            self.__connection = pika.BlockingConnection(parameters)

            self.publish_online()

        return self.__connection

    @property
    def channel(self):
        if self.__channel is None:
            self.__channel = self.connection.channel()
            self.__channel.queue_declare(queue='clique-response')

        return self.__channel

    @property
    def command_queue(self):
        self.channel.queue_declare(queue=self.COMMAND_QUEUE_NAME)
        return self.COMMAND_QUEUE_NAME

    @property
    def status_exchange(self):
        self.channel.exchange_declare(exchange=self.STATUS_EXCHANGE_NAME,
                                      exchange_type='fanout')
        return self.STATUS_EXCHANGE_NAME

    @property
    def status_queue(self):
        queue = self.channel.queue_declare(
            queue=self.STATUS_QUEUE_NAME % self.uuid,
            exclusive=True
        )
        self.channel.queue_bind(exchange=self.status_exchange,
                                routing_key='',
                                queue=queue.method.queue)
        return queue.method.queue

    def encode_message(self, **kwargs):
        props = dict(uuid=self.uuid,
                     time=time(),
                     **kwargs)
        checksum = md5(json.dumps(props).encode('utf8')).hexdigest()
        props['checksum'] = checksum
        return checksum, json.dumps(props)

    def publish(self, key, **kwargs):
        checksum, message = self.encode_message(**kwargs)
        self.channel.basic_publish(exchange='',
                                   routing_key='clique-%s' % key,
                                   body=message)
        return checksum

    def publish_online(self):
        return self.publish_stats(uname=uname())

    def publish_command(self, command, **kwargs):
        checksum, message = self.encode_message(command=command, **kwargs)
        self.channel.basic_publish(exchange='',
                                   routing_key=self.command_queue,
                                   body=message)
        return checksum

    def publish_response(self, command, **kwargs):
        return self.publish('response', command=command, **kwargs)

    def publish_stats(self, **stats):
        checksum, message = self.encode_message(**stats)
        self.channel.basic_publish(exchange=self.status_exchange,
                                   routing_key='',
                                   body=message)
        return checksum

    def ack_message(self, tag):
        self.channel.basic_ack(tag)

    def decode_message(self, callback, channel, method, properties, body):
        try:
            callback((method.delivery_tag,
                      json.loads(body.decode('utf8'))))
        except:
            self.ack_message(method.delivery_tag)

    def create_listener(self, kwargs, observer):
        try:
            self.channel.basic_consume(partial(self.decode_message,
                                               observer.on_next),
                                       **kwargs)
            self.channel.basic_qos(prefetch_count=1)
            self.channel.start_consuming()
        except Exception as error:
            self.channel.stop_consuming()
            observer.on_error(error)

        observer.on_completed()

    def get_listener(self, key):
        return Observable.create(partial(self.create_listener,
                                         dict(queue='clique-%s' % key))) \
            .where(lambda m: m is not None)

    def get_status_listener(self):
        return Observable.create(partial(self.create_listener,
                                         dict(queue=self.status_queue))) \
            .where(lambda m: m is not None) \
            .tap(lambda r: self.ack_message(r[0]))

    def get_command_listener(self):
        return Observable.create(partial(self.create_listener,
                                         dict(queue=self.command_queue))) \
            .where(lambda m: m is not None)

    def get_response_listener(self):
        return self.get_listener('response')

    def stop_listeners(self):
        self.channel.stop_consuming()
