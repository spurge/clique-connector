# -*- coding: utf-8 -*-

import json
import pika

from ipgetter import myip
from os import uname
from time import time
from uuid import getnode


def encode_message(**kwargs):
    return json.dumps(dict(**kwargs))


class Messenger:

    def __init__(self):
        self.uuid = None

    def get_uuid(self):
        if self.uuid is None:
            self.uuid = str(getnode())

        return self.uuid

    async def connect(self):
        parameters = pika.URLParameters(self.url)
        self.connection = pika.BlockingConnection(parameters)

    async def set_channels(self):
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='clique-commands')
        self.channel.queue_declare(queue='clique-responses')
        self.channel.queue_declare(queue='clique-status')

    async def publish_online(self):
        self.channel.basic_publish(exchange='',
                                   routing_key='clique-status',
                                   body=encode_message(uuid=self.get_uuid(),
                                                       host=uname(),
                                                       time=time()))

    async def publish_command(self, command, **kwargs):
        self.channel.basic_publish(exchange='',
                                   routing_key='clique-commands',
                                   body=encode_message(uuid=self.get_uuid(),
                                                       command=command,
                                                       **kwargs))

    async def listen_for_commands(self, ):
        pass
