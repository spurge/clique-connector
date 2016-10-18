# -*- coding: utf-8 -*-

import asyncio

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
                        username='testuser',
                        password='password')

        loop = asyncio.get_event_loop()
        req = self.connector.create_machine('testmachine',
                                            'alpine',
                                            1,
                                            512)
        res = self.connector.wait_for_machine(callback)
        loop.run_until_complete(asyncio.wait([req, res]))
