# -*- coding: utf-8 -*-

import asyncio

from unittest import TestCase

from messenger import Messenger


class TestMessenger(TestCase):

    def setUp(self):
        self.loop = asyncio.get_event_loop()
        self.messenger = Messenger('amqp://localhost')

    def tearDown(self):
        if self.messenger.connection is not None:
            self.messenger.connection.close()

    def test_connect(self):
        self.loop.run_until_complete(asyncio.wait(
            self.messenger.connect()
        ))

    def test_command(self):
        future = asyncio.Future()

        def callback(command):
            print(command)
            future.set_result(command)

        async def timeout():
            await asyncio.sleep(10)
            future.set_exception(Exception('Timed out'))

        self.loop.run_until_complete(asyncio.wait([
            future,
            self.messenger.listen_for_commands(callback),
            self.messenger.publish_command('test'),
            timeout()
        ]))
