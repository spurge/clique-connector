# -*- coding: utf-8 -*-

import asyncio

from functools import partial
from unittest import TestCase

from messenger import Messenger


def stop(messenger, _):
    messenger.stop_listeners()


def error(messenger, error):
    messenger.stop_listeners()
    raise error


class TestMessenger(TestCase):

    def setUp(self):
        self.messenger = Messenger('amqp://localhost')

    def tearDown(self):
        if self.messenger.connection is not None:
            self.messenger.connection.close()

    def test_online(self):
        loop = asyncio.get_event_loop()

        self.assertEqual(self.messenger.status_queue,
                         Messenger.STATUS_QUEUE_NAME % self.messenger.uuid)
        self.assertIsInstance(self.messenger.publish_online(), str)

        tag, status = loop.run_until_complete(
            self.messenger
                .get_status_listener()
                .where(lambda r: r[1]['uuid'] == self.messenger.uuid)
                .tap(partial(stop, self.messenger))
                .timeout(3000)
                .catch_exception(partial(error, self.messenger))
        )

        self.assertIsInstance(tag, int)
        self.assertIsInstance(status['uname'], list)
        self.assertIsInstance(status['time'], float)
        self.assertIsInstance(status['checksum'], str)
        self.assertEqual(status['uuid'], self.messenger.uuid)

    def test_command(self):
        checksum = self.messenger.publish_command('test',
                                                  arg='value')
        loop = asyncio.get_event_loop()

        tag, command = loop.run_until_complete(
            self.messenger
            .get_command_listener()
            .where(
                lambda c:
                    c[1]['checksum'] == checksum and
                    c[1]['command'] == 'test'
            )
            .tap(partial(stop, self.messenger))
            .timeout(3000)
            .catch_exception(partial(error, self.messenger))
        )

        self.assertIsInstance(tag, int)
        self.messenger.ack_message(tag)

        self.assertEqual(command['command'], 'test')
        self.assertEqual(command['arg'], 'value')
        self.assertEqual(command['uuid'], self.messenger.uuid)
        self.assertIsInstance(command['time'], float)
