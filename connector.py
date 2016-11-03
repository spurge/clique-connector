# -*- coding: utf-8 -*-

"""
Copyright (c) 2016 Olof Montin <olof@montin.net>

This file is part of clique-connector.
"""

import logging

from functools import partial
from rx import Observable
from rx.concurrency import AsyncIOScheduler

from messenger import Messenger
from util import listener_error, filter_message


class Connector:
    """Wraps the Messenger and extends it's listener observables for
    creating the logistics of creating and responding with virtual
    machines.

    Use like so:
        connector = Connector('127.0.0.1')

        # Request a virtual machine
        machine = await connector.create_machine(
            'some-random-machine', # Machine name
            'ubuntu-16.04', # Image name
            1, # CPU
            512, # Memory in MB
            128, # Disc in GB
            'your-public-ssh-key-as-string') # Public SSH key

        # Response with a virtual machine
        def callback(channel_close, name, cpu, mem, disc, pkey):
            return dict(host='127.0.0.1',
                        username='root')

        # Create listener
        channel_close, observable = connector.wait_for_machines(callback)

        # Start listening
        machines = await observable \
            .take(3) # Create only three machines

        # Clean up by closing channel
        channel_close()
    """

    def __init__(self, host):
        self.host = host
        self.__messenger = None

    @property
    def messenger(self):
        """Sets and returns a messenger instance
        """

        if self.__messenger is None:
            self.__messenger = Messenger(self.host)

        return self.__messenger

    def get_response(self, timeout, checksum):
        """Creates a listener for a single response by provided checksum.
        The timeout sets how long the listener should wait.
        Returns the first response message.
        """

        stop, observable = self.messenger \
                               .get_response_listener(checksum)

        return observable \
            .first() \
            .tap(lambda m: m.ack()) \
            .tap(lambda _: stop()) \
            .timeout(timeout) \
            .catch_exception(partial(listener_error, stop))

    def publish_response(self, kwargs, message):
        """Publish a response and returns with the message checksum.
        """

        body = message.json()

        return self.messenger.publish_response(body['uuid'],
                                               body['checksum'],
                                               **kwargs)

    def create_machine(self, name, image, cpu,
                       mem, disc, pkey, retries=0,
                       scheduler=AsyncIOScheduler()):
        """Creates a create-machine request and listens for a response.
        Returns with an observable which generates a single value with
        the virtual machine. The machine response is a dict:
            { 'host': '127.0.0.1',
              'username': 'root' }
        """

        def retry(error):
            """The built-in retry in ReactiveX whouldn't do it,
            so I hade write this in order to retry the while observable
            chain.
            """

            if retries < 10:
                logging.warning('Retrying request machine: %d/10',
                                retries + 1)

                return self.create_machine(
                    name, image, cpu, mem, disc, pkey, retries + 1,
                    scheduler)

            raise error

        return Observable.start(partial(
                self.messenger.publish_command,
                command='machine-requested',
                name=name,
                image=image,
                cpu=cpu,
                mem=mem,
                disc=disc,
                pkey=pkey), scheduler=scheduler) \
            .tap(lambda cs: logging.debug('Machine requested: %s',
                                          cs)) \
            .flat_map(
                # Wait for the first response for machine-request
                # command.
                partial(self.get_response, 5000)) \
            .tap(lambda m: logging.debug('Machine confirmed %s',
                                         m.body)) \
            .map(
                # Confirm the response and make the agent actually
                # create the virtual machine.
                partial(self.publish_response, {})) \
            .flat_map(
                # Wait for the machine...
                partial(self.get_response, 5000)) \
            .map(lambda m: m.json()) \
            .tap(partial(logging.debug, 'Machine response: %s')) \
            .map(
                # Map the machine message for something useful.
                lambda m: dict(host=m['host'],
                               username=m['username'])) \
            .catch_exception(retry) \
            .first()

    def stop_machine(self, name):
        pass

    def send_stats(self):
        pass

    def wait_for_machines(self, callback):
        """Creates a command listener for incoming machine requests.
        The callback is used to create the actual virtual machine.
        Returns a channel close function and the listener observable.
        """

        stop, observable = self.messenger.get_command_listener()

        def handle_error(message, error):
            """If by any reason the communication between the listener
            (agent) and the requester (api) whould brake, this error
            catcher will just acknowledge the message and continue
            listening.
            """

            logging.error(
                'Error while listening for machines: %s',
                error)
            message.ack()
            return Observable.just(None)

        return stop, observable \
            .where(partial(filter_message,
                           dict(command='machine-requested'))) \
            .tap(lambda m: logging.debug('Machine requested: %s',
                                         m.body)) \
            .flat_map(
                # Incoming machine request
                lambda cm:
                    # Response with your availability
                    Observable.just(self.publish_response({}, cm))
                    # Listens for a confirm
                    .flat_map(partial(self.get_response, 1000))
                    .tap(
                        lambda m:
                            logging.debug('Machine confirmed: %s',
                                          m.body))
                    .map(lambda m: (cm.json(), m))
                    # Create the actual machine
                    .map(lambda cm_m: (callback(stop,
                                                cm_m[0]['name'],
                                                cm_m[0]['image'],
                                                cm_m[0]['cpu'],
                                                cm_m[0]['mem'],
                                                cm_m[0]['disc'],
                                                cm_m[0]['pkey']),
                                       cm_m[1]))
                    .tap(
                        lambda vm_m:
                        logging.debug('Responding with machine: %s',
                                      vm_m[0]))
                    # Respond with the machine
                    .map(lambda vm_m: self.publish_response(*vm_m))
                    .tap(lambda _: cm.ack())
                    .catch_exception(partial(handle_error, cm))
                    .where(lambda m: m is not None)
            ) \
            .catch_exception(partial(listener_error, stop))

    def listen_for_stats(self):
        pass
