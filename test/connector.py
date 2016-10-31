# -*- coding: utf-8 -*-

import asyncio
import logging

from functools import partial
from rx import Observable
from unittest import TestCase
from unittest.mock import patch, Mock

from connector import Connector
from util import listener_error


logging.basicConfig(level=logging.DEBUG)


class TestConnector(TestCase):

    def setUp(self):
        self.connector = Connector('127.0.0.1')

    def tearDown(self):
        self.connector.messenger.connection.close()

    def test_create_machine(self):
        def callback(stop, name, image, cpu, mem, disc, pkey):
            stop()

            return dict(host='testhost',
                        username='testuser')

        loop = asyncio.get_event_loop()
        stop, observable = self.connector.wait_for_machines(callback)

        result, _ = loop.run_until_complete(asyncio.wait([
            self.connector.create_machine(
                'testmachine',
                'alpine',
                1,
                512,
                128,
                'public-key'
            ),
            observable
            .first()
            .catch_exception(partial(listener_error, stop))
        ]))

        (create,) = (r.result() for r in result if 'host' in r.result())

        self.assertEqual(create['host'], 'testhost')
        self.assertEqual(create['username'], 'testuser')

    @patch('messenger.Messenger.fetch')
    @patch('messenger.Messenger.publish')
    def test_raceconditioned_create_machine(self, publish, fetch):
        messenger = MessengerMock(self.connector.messenger)
        messenger.publish(publish)
        messenger.fetch(fetch)

        def callback(name, cpu, mem):
            return dict(host='testhost',
                        username='testuser')

        loop = asyncio.get_event_loop()

        result, _ = loop.run_until_complete(asyncio.wait([
            self.connector.create_machine(
                'testmachine',
                'alpine',
                1,
                512,
                'public-key'
            )
            .timeout(5000),
            self.connector.wait_for_machines(callback)
            .first()
            .timeout(5000)
            .catch_exception(lambda _: Observable.just('failed')),
            self.connector.wait_for_machines(callback)
            .first()
            .timeout(5000)
            .catch_exception(lambda _: Observable.just('failed'))
        ]))

        failed = [r.result()
                  for r in result
                  if r.result() == 'failed']
        self.assertEqual(len(failed), 1)

    def test_failing_listener_with_fallback(self):
        def callback_ok(name, cpu, mem):
            return dict(host='testhost',
                        username='testuser')

        def callback_fail(name, cpu, mem):
            return dict(test='ok')

        callback = Mock(side_effect=[callback_fail, callback_ok])

        loop = asyncio.get_event_loop()

        connector1 = Connector('amqp://localhost')
        connector2 = Connector('amqp://localhost')

        result, _ = loop.run_until_complete(asyncio.wait([
            self.connector.create_machine(
                'testmachine',
                'alpine',
                1,
                512,
                'public-key'
            )
            .timeout(10000),
            connector1.wait_for_machines(callback)
            .first()
            .timeout(10000),
            connector2.wait_for_machines(callback)
            .first()
            .timeout(10000)
        ]))

        print(result)
