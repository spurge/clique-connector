# -*- coding: utf-8 -*-

import asyncio

from rx import Observable
from unittest import TestCase

from connector import Connector


class TestConnector(TestCase):

    def setUp(self):
        self.connector = Connector('amqp://localhost')

    def tearDown(self):
        self.connector.messenger.connection.close()

    def test_create_machine(self):
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
        ]))

        (create,) = (r.result() for r in result if 'host' in r.result())

        self.assertEqual(create['host'], 'testhost')
        self.assertEqual(create['username'], 'testuser')

    def test_raceconditioned_create_machine(self):
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
