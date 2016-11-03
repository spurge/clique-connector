# -*- coding: utf-8 -*-

import asyncio
import logging

from functools import partial
from rx import Observable
from unittest import TestCase
from unittest.mock import Mock

from clique_connector import Connector
from clique_connector.util import listener_error


logging.basicConfig(level=logging.DEBUG)


class TestConnector(TestCase):

    def setUp(self):
        self.connector = Connector('127.0.0.1')

    def tearDown(self):
        self.connector.messenger.connection.close()

    def test_create_machine(self):
        def callback(stop, name, image, cpu, mem, disc, pkey):
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
            .tap(lambda _: stop())
            .catch_exception(partial(listener_error, stop))
        ]))

        created = [r.result() for r in result
                   if type(r.result()) is dict][0]

        self.assertEqual(created['host'], 'testhost')
        self.assertEqual(created['username'], 'testuser')

    def test_raceconditioned_create_machine(self):
        def callback(stop, name, image, cpu, mem, disc, pkey):
            return dict(host='testhost',
                        username='testuser')

        loop = asyncio.get_event_loop()
        connector_1 = Connector('127.0.0.1')
        stop_1, observable_1 = connector_1.wait_for_machines(callback)
        connector_2 = Connector('127.0.0.1')
        stop_2, observable_2 = connector_2.wait_for_machines(callback)

        result, _ = loop.run_until_complete(asyncio.wait([
            self.connector.create_machine(
                'testmachine',
                'alpine',
                1,
                512,
                128,
                'public-key'
            ),
            observable_1
            .first()
            .tap(lambda _: stop_1())
            .timeout(5000)
            .catch_exception(partial(listener_error, stop_1))
            .catch_exception(lambda _: Observable.just('failed')),
            observable_2
            .first()
            .tap(lambda _: stop_2())
            .timeout(5000)
            .catch_exception(partial(listener_error, stop_2))
            .catch_exception(lambda _: Observable.just('failed'))
        ]))

        failed = [r.result()
                  for r in result
                  if r.result() == 'failed']
        self.assertEqual(len(failed), 1)

        created = [r.result() for r in result
                   if type(r.result()) is dict][0]

        self.assertEqual(created['host'], 'testhost')
        self.assertEqual(created['username'], 'testuser')

    def test_failing_listener_with_fallback(self):
        callback = Mock(side_effect=[dict(failing=True),
                                     Exception('Oups'),
                                     dict(host='testhost',
                                          username='testuser')])
        loop = asyncio.get_event_loop()

        connector_1 = Connector('127.0.0.1')
        stop_1, observable_1 = connector_1.wait_for_machines(callback)

        connector_2 = Connector('127.0.0.1')
        stop_2, observable_2 = connector_2.wait_for_machines(callback)

        result, _ = loop.run_until_complete(asyncio.wait([
            self.connector.create_machine(
                'testmachine',
                'alpine',
                1,
                512,
                128,
                'public-key'
            ),
            observable_1
            .first()
            .tap(lambda _: stop_1())
            .timeout(20000)
            .catch_exception(partial(listener_error, stop_1))
            .catch_exception(lambda _: Observable.just('failed')),
            observable_2
            .take(2)
            .tap(lambda _: stop_2())
            .timeout(20000)
            .catch_exception(partial(listener_error, stop_2))
            .catch_exception(lambda _: Observable.just('failed'))
        ]))

        failed = [r.result()
                  for r in result
                  if r.result() == 'failed']
        self.assertEqual(len(failed), 1)

        created = [r.result() for r in result
                   if type(r.result()) is dict][0]

        self.assertEqual(created['host'], 'testhost')
        self.assertEqual(created['username'], 'testuser')
