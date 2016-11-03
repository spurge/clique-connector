# -*- coding: utf-8 -*-

import asyncio
import logging

from functools import partial
from unittest import TestCase

from clique_connector import Messenger
from clique_connector.util import listener_error, filter_message


logging.basicConfig(level=logging.DEBUG)


def get_response(messenger, checksum, test_values, response_checksum):
    stop, observable = messenger.get_response_listener(checksum)

    return observable \
        .where(partial(filter_message,
                       dict(checksum=response_checksum,
                            **test_values))) \
        .tap(lambda m: m.ack()) \
        .first() \
        .timeout(3000) \
        .tap(lambda _: stop()) \
        .catch_exception(partial(listener_error, stop))


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
            .tap(lambda m: m.ack())
            .map(lambda m: m.json())
            .where(
                lambda m:
                    m['uuid'] == messenger.uuid)
            .first()
            .tap(lambda _: stop())
            .timeout(3000)
            .catch_exception(partial(listener_error, stop)))

        self.assertIsInstance(status['uname'], list)
        self.assertIsInstance(status['time'], float)
        self.assertIsInstance(status['checksum'], str)
        self.assertEqual(status['uuid'], messenger.uuid)

    def test_command(self):
        stop, observable = self.messenger.get_command_listener()
        checksum = self.messenger.publish_command('test',
                                                  arg='value')
        loop = asyncio.get_event_loop()

        command = loop.run_until_complete(
            observable
            .tap(lambda m: m.ack())
            .map(lambda m: m.json())
            .where(
                lambda m:
                    m['command'] == 'test' and
                    m['checksum'] == checksum
                )
            .first()
            .timeout(3000)
            .tap(lambda _: stop())
            .catch_exception(partial(listener_error, stop)))

        self.assertEqual(command['command'], 'test')
        self.assertEqual(command['arg'], 'value')
        self.assertEqual(command['uuid'], self.messenger.uuid)
        self.assertIsInstance(command['time'], float)

    def test_response(self):
        stop, observable = self.messenger.get_command_listener()
        checksum = self.messenger.publish_command('test',
                                                  arg='value')
        loop = asyncio.get_event_loop()

        response = loop.run_until_complete(
            observable
            .tap(lambda m: m.ack())
            .where(partial(filter_message, dict(checksum=checksum)))
            .first()
            .map(
                lambda m:
                    self.messenger.publish_response(
                        m.json()['uuid'],
                        m.json()['checksum'],
                        response='value',
                        test_checksum=checksum))
            .flat_map(partial(get_response,
                              self.messenger,
                              checksum,
                              dict(test_checksum=checksum)))
            .map(lambda m: m.json())
            .timeout(3000)
            .tap(lambda _: stop())
            .catch_exception(partial(listener_error, stop)))

        self.assertEqual(response['response'], 'value')
        self.assertEqual(response['test_checksum'], checksum)
        self.assertEqual(response['uuid'], self.messenger.uuid)
        self.assertIsInstance(response['time'], float)
