# -*- coding: utf-8 -*-

import asyncio
import json
import logging

from functools import partial
from unittest import TestCase

from messenger import Messenger


logging.basicConfig(level=logging.DEBUG)


def error(stop, error):
    stop()
    raise error


class TestMessenger(TestCase):

    def setUp(self):
        self.messenger = Messenger('127.0.0.1')

    def tearDown(self):
        if self.messenger.connection is not None:
            self.messenger.connection.close()

    def test_online(self):
        loop = asyncio.get_event_loop()
        stop, observable = self.messenger.get_status_listener()

        messenger = Messenger('127.0.0.1')

        status = loop.run_until_complete(
            observable
            .map(lambda m: json.loads(m.body))
            .where(
                lambda m:
                    m['uuid'] == messenger.uuid)
            .first()
            .timeout(3000)
            .tap(lambda _: stop())
            .catch_exception(partial(error, stop)))

        self.assertIsInstance(status['uname'], list)
        self.assertIsInstance(status['time'], float)
        self.assertIsInstance(status['checksum'], str)
        self.assertEqual(status['uuid'], messenger.uuid)

    def test_command(self):
        checksum = self.messenger.publish_command('test',
                                                  arg='value')
        loop = asyncio.get_event_loop()

        tag, command = loop.run_until_complete(
            self.messenger
                .get_command_listener()
                .where(
                    lambda c:
                        c[1]['command'] == 'test' and
                        c[1]['checksum'] == checksum
                    )
                .tap(lambda c: c[2]())
                .timeout(3000)
                .catch_exception(partial(error, self.messenger))
        )

        self.assertIsInstance(tag, int)
        self.messenger.ack_message(tag)

        self.assertEqual(command['command'], 'test')
        self.assertEqual(command['arg'], 'value')
        self.assertEqual(command['uuid'], self.messenger.uuid)
        self.assertIsInstance(command['time'], float)

    def test_response(self):
        checksum = self.messenger.publish_command('test',
                                                  arg='value')
        loop = asyncio.get_event_loop()

        tag, response = loop.run_until_complete(
            self.messenger
                .get_command_listener()
                .where(
                    lambda c:
                        c[1]['command'] == 'test' and
                        c[1]['checksum'] == checksum
                    )
                .tap(print)
                .map(
                    lambda c:
                        self.messenger.publish_response(
                            c[1]['uuid'],
                            c[1]['checksum'],
                            response='value',
                            test_checksum=checksum
                        )
                    )
                .tap(print)
                .flat_map(
                    lambda cs:
                        self.messenger.get_response_listener(checksum)
                            .tap(print)
                            .where(
                                lambda r:
                                    r[1]['test_checksum'] == checksum
                            )
                    )
                .tap(partial(stop, self.messenger))
                .timeout(3000)
                .catch_exception(partial(error, self.messenger))
        )

        print(response)
