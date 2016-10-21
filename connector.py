# -*- coding: utf-8 -*-

from rx import Observable

from messenger import Messenger


class Connector:

    def __init__(self, url):
        self.url = url
        self.__messenger = None

    @property
    def messenger(self):
        if self.__messenger is None:
            self.__messenger = Messenger(self.url)

        return self.__messenger

    def create_machine(self, name, image, cpu, mem, public_key):
        response_listener = self.messenger.get_response_listener()

        return Observable.just(self.messenger.publish_command(
                command='machine-requested',
                name=name,
                image=image,
                cpu=cpu,
                mem=mem,
                pkey=public_key
            )) \
            .flat_map(
                lambda cs:
                    response_listener
                    .where(lambda r: r['requested_checksum'] == cs)
                    .first()
                    .timeout(10000)
                    .flat_map(
                        lambda r:
                            Observable.just(
                                self.messenger.publish_command(
                                    'machine-confirmed',
                                    confirmed_checksum=r['checksum']
                                )
                            )
                    )
                    .flat_map(
                        lambda cs:
                            response_listener
                            .where(
                                lambda r:
                                    r['requested_checksum'] == cs
                            )
                            .first()
                            .timeout(10000)
                    )
                    .retry(10)
            ) \
            .map(lambda r: dict(host=r['host'],
                                username=r['username']))

    def stop_machine(self, name):
        pass

    def send_stats(self):
        pass

    def wait_for_machines(self, callback):
        command_listener = self.messenger.get_command_listener()

        return command_listener \
            .where(lambda cr: cr['command'] == 'machine-requested') \
            .flat_map(
                lambda cr:
                    Observable.just(self.messenger.publish_response(
                        'machine-requested',
                        requested_checksum=cr['checksum']
                    ))
                    .flat_map(
                        lambda cs:
                            command_listener
                            .where(
                                lambda cc:
                                    cc['confirmed_checksum'] == cs
                            )
                            .first()
                            .map(
                                lambda cc:
                                    self.messenger.publish_response(
                                        'machine-confirmed',
                                        requested_checksum=cc['checksum'],
                                        **(callback(
                                            cr['name'],
                                            cr['cpu'],
                                            cr['mem']
                                        ))
                                    )
                            )
                    )
            )

    def listen_for_stats(self):
        pass
