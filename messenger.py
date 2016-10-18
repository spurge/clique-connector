# -*- coding: utf-8 -*-

import json
import pika

from hashlib import md5
from os import uname
from rx import Observable
from rx.concurrency import AsyncIOScheduler
from time import time
from uuid import getnode


class Messenger:

    def __init__(self, url):
        self.url = url
        self.__uuid = None
        self.__connection = None
        self.__channel = None

    @property
    def uuid(self):
        if self.__uuid is None:
            self.__uuid = str(getnode())

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
            self.__channel.queue_declare(queue='clique-command')
            self.__channel.queue_declare(queue='clique-response')
            self.__channel.queue_declare(queue='clique-status')

        return self.__channel

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
        return self.publish('status', uname=uname())

    def publish_command(self, command, **kwargs):
        return self.publish('command', command=command, **kwargs)

    def publish_response(self, command, **kwargs):
        return self.publish('response', command=command, **kwargs)

    def publish_stats(self, stats):
        return self.publish('status', stats=stats)

    def fetch(self, key):
        try:
            m, p, b = self.channel.basic_get(queue='clique-%s' % key)

            if b is not None:
                self.channel.basic_ack(m.delivery_tag)
                return json.loads(b.decode('utf8'))
        except:
            pass

    def get_listener(self, key):
        return Observable.interval(1000,
                                   scheduler=AsyncIOScheduler()) \
            .map(lambda _: self.fetch(key)) \
            .where(lambda c: c is not None)

    def get_status_listener(self):
        return self.get_listener('status')

    def get_command_listener(self):
        return self.get_listener('command')

    def get_response_listener(self):
        return self.get_listener('response')
