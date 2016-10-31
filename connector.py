# -*- coding: utf-8 -*-

import logging

from functools import partial
from rx import Observable

from messenger import Messenger
from util import listener_error, filter_message


class Connector:

    def __init__(self, host):
        self.host = host
        self.__messenger = None

    @property
    def messenger(self):
        if self.__messenger is None:
            self.__messenger = Messenger(self.host)

        return self.__messenger

    def get_response(self, timeout, checksum):
        stop, observable = self.messenger \
                           .get_response_listener(checksum)

        return observable \
            .first() \
            .tap(lambda m: m.ack()) \
            .tap(lambda _: stop()) \
            .catch_exception(partial(listener_error, stop))

    def publish_response(self, kwargs, message):
        body = message.json()

        return self.messenger.publish_response(body['uuid'],
                                               body['checksum'],
                                               **kwargs)

    def create_machine(self, name, image, cpu, mem, disc, pkey):
        return Observable.just(self.messenger.publish_command(
                command='machine-requested',
                name=name,
                image=image,
                cpu=cpu,
                mem=mem,
                disc=disc,
                pkey=pkey
            )) \
            .flat_map(partial(self.get_response, 10000)) \
            .map(partial(self.publish_response, {})) \
            .flat_map(partial(self.get_response, 10000)) \
            .map(lambda m: m.json()) \
            .map(lambda m: dict(host=m['host'],
                                username=m['username']))

    def stop_machine(self, name):
        pass

    def send_stats(self):
        pass

    def wait_for_machines(self, callback):
        stop, observable = self.messenger.get_command_listener()

        def handle_error(message, error):
            logging.error('Error while listening for machines: %s',
                          message.body,
                          extra=error)
            message.ack()

        return stop, observable \
            .where(partial(filter_message,
                           dict(command='machine-requested'))) \
            .flat_map(
                lambda cm:
                    Observable.just(self.publish_response({}, cm))
                    .flat_map(partial(self.get_response, 1000))
                    .map(lambda _: callback(stop,
                                            cm['name'],
                                            cm['image'],
                                            cm['cpu'],
                                            cm['mem'],
                                            cm['disc'],
                                            cm['pkey']))
                    .map(lambda vm: self.publish_response(vm, cm))
                    .tap(lambda _: cm.ack())
                    .catch_exception(partial(handle_error, cm))
            ) \
            .catch_exception(partial(listener_error, stop))

    def listen_for_stats(self):
        pass
