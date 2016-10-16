# -*- coding: utf-8 -*-

import json
import pika

from os import uname
from rx import Observable
from time import time
from uuid import getnode


class Messenger:

    def __init__(self, url):
        self.url = url
        self.uuid = None
        self.connection = None
        self.channel = None

    def get_uuid(self):
        if self.uuid is None:
            self.uuid = str(getnode())

        return self.uuid

    def encode_message(self, **kwargs):
        return json.dumps(dict(uuid=self.get_uuid(),
                               **kwargs))

    def connect(self):
        if self.connection is None:
            parameters = pika.URLParameters(self.url)
            self.connection = pika.BlockingConnection(parameters)

            self.set_channels()
            self.publish_online()

    def set_channels(self):
        if self.channel is None:
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue='clique-commands')
            self.channel.queue_declare(queue='clique-responses')
            self.channel.queue_declare(queue='clique-status')

    def publish_online(self):
        self.connect()
        self.channel.basic_publish(exchange='',
                                   routing_key='clique-status',
                                   body=self.encode_message(host=uname(),
                                                            time=time()))

    def publish_command(self, command, **kwargs):
        self.connect()
        self.channel.basic_publish(exchange='',
                                   routing_key='clique-commands',
                                   body=self.encode_message(command=command,
                                                            **kwargs))

    def publish_stats(self, stats):
        self.connect()
        self.channel.basic_publish(exchange='',
                                   routing_key='clique-status',
                                   body=self.encode_message(stats=stats))

    def fetch_command(self):
        try:
            m, p, b = self.channel.basic_get(queue='clique-commands')

            if b is not None:
                self.channel.basic_ack(m.delivery_tag)
                return b.decode('utf8')
        except:
            pass

    def get_command_listener(self):
        self.connect()

        return Observable.interval(
            1000
        ).map(
            lambda _: self.fetch_command()
        ).where(
            lambda data: data is not None
        )
