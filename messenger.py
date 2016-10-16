# -*- coding: utf-8 -*-

import asyncio
import json
import pika

from os import uname
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

    async def connect(self):
        if self.connection is None:
            parameters = pika.URLParameters(self.url)
            self.connection = pika.BlockingConnection(parameters)

            await self.set_channels()
            await self.publish_online()

    async def set_channels(self):
        if self.channel is None:
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue='clique-commands')
            self.channel.queue_declare(queue='clique-responses')
            self.channel.queue_declare(queue='clique-status')

    async def publish_online(self):
        await self.connect()
        self.channel.basic_publish(exchange='',
                                   routing_key='clique-status',
                                   body=self.encode_message(host=uname(),
                                                            time=time()))

    async def publish_command(self, command, **kwargs):
        await self.connect()
        self.channel.basic_publish(exchange='',
                                   routing_key='clique-commands',
                                   body=self.encode_message(command=command,
                                                            **kwargs))

    async def publish_stats(self, stats):
        await self.connect()
        self.channel.basic_publish(exchange='',
                                   routing_key='clique-status',
                                   body=self.encode_message(stats=stats))

    async def listen_for_commands(self, callback):
        await self.connect()

        while True:
            try:
                m, p, b = self.channel.basic_get(queue='clique-commands')

                if b is not None:
                    callback(b.decode())
                    self.channel.basic_ack(m.delivery_tag)
            except:
                pass

            await asyncio.sleep(1)
