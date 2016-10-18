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

    def test_online(self):
        async def test():
            result = await self.messenger \
                .get_status_listener() \
                .first() \
                .timeout(3000)

            self.assertIsInstance(result['uname'], list)
            self.assertIsInstance(result['time'], float)
            self.assertIsInstance(result['checksum'], str)
            self.assertEqual(result['uuid'], self.messenger.uuid)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(test())

    def test_command(self):
        checksum = self.messenger.publish_command('test',
                                                  arg='value')

        async def test():
            result = await self.messenger \
                .get_command_listener() \
                .where(lambda c: c['checksum'] == checksum) \
                .first() \
                .timeout(3000)

            self.assertEqual(result['command'], 'test')
            self.assertEqual(result['arg'], 'value')
            self.assertEqual(result['uuid'], self.messenger.uuid)
            self.assertIsInstance(result['time'], float)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(test())
