# -*- coding: utf-8 -*-

import logging

from functools import partial
from rx import Observable
from rx.concurrency import AsyncIOScheduler

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
            .timeout(timeout) \
            .catch_exception(partial(listener_error, stop))

    def publish_response(self, kwargs, message):
        body = message.json()

        return self.messenger.publish_response(body['uuid'],
                                               body['checksum'],
                                               **kwargs)

    def create_machine(self, name, image, cpu,
                       mem, disc, pkey, retries=0,
                       scheduler=AsyncIOScheduler()):
        def retry(error):
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
            .flat_map(partial(self.get_response, 5000)) \
            .tap(lambda m: logging.debug('Machine confirmed %s',
                                         m.body)) \
            .map(partial(self.publish_response, {})) \
            .flat_map(partial(self.get_response, 5000)) \
            .map(lambda m: m.json()) \
            .tap(partial(logging.debug, 'Machine response: %s')) \
            .map(lambda m: dict(host=m['host'],
                                username=m['username'])) \
            .catch_exception(retry) \
            .first()

    def stop_machine(self, name):
        pass

    def send_stats(self):
        pass

    def wait_for_machines(self, callback):
        stop, observable = self.messenger.get_command_listener()

        def handle_error(message, error):
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
                lambda cm:
                    Observable.just(self.publish_response({}, cm))
                    .flat_map(partial(self.get_response, 1000))
                    .tap(
                        lambda m:
                            logging.debug('Machine confirmed: %s',
                                          m.body))
                    .map(lambda m: (cm.json(), m))
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
                    .map(lambda vm_m: self.publish_response(*vm_m))
                    .tap(lambda _: cm.ack())
                    .catch_exception(partial(handle_error, cm))
                    .where(lambda m: m is not None)
            ) \
            .catch_exception(partial(listener_error, stop))

    def listen_for_stats(self):
        pass
