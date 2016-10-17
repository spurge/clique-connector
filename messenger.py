# -*- coding: utf-8 -*-

import json
import pika

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
        return json.dumps(dict(uuid=self.uuid,
                               **kwargs))

    def publish(self, key, **kwargs):
        self.channel.basic_publish(exchange='',
                                   routing_key='clique-%s' % key,
                                   body=self.encode_message(**kwargs))

    def publish_online(self):
        self.publish('status', host=uname(), time=time())

    def publish_command(self, command, **kwargs):
        self.publish('command', command=command, **kwargs)

    def publish_stats(self, stats):
        self.publish('status', stats=stats)

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
