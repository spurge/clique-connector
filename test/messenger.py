# -*- coding: utf-8 -*-

import asyncio

from unittest import TestCase

from messenger import Messenger


class TestMessenger(TestCase):

    def setUp(self):
        self.messenger = Messenger('amqp://localhost')

    def tearDown(self):
        if self.messenger.connection is not None:
            self.messenger.connection.close()

    def test_command(self):
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        disposable = self.messenger.get_command_listener().timeout(
            3000
        ).subscribe(
            lambda c: not future.done() and future.set_result(c),
            lambda e: loop.stop()
        )

        self.messenger.publish_command('test')

        loop.run_until_complete(future)
        disposable.dispose()
