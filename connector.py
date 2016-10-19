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
                    self.messenger.get_response_listener()
                    .where(lambda r: r['requested_checksum'] == cs)
            ) \
            .first() \
            .map(lambda r: self.messenger.publish_command(
                'machine-confirmed',
                confirmed_checksum=r['checksum']
            )) \
            .flat_map(
                lambda cs:
                    self.messenger.get_response_listener()
                    .where(
                        lambda r:
                            r['requested_checksum'] == cs
                    )
            ) \
            .first() \
            .map(lambda r: dict(host=r['host'],
                                username=r['username'])) \

    def stop_machine(self, name):
        pass

    def send_stats(self):
        pass

    def wait_for_machines(self, callback):
        return self.messenger.get_command_listener() \
            .where(lambda c: c['command'] == 'machine-requested') \
            .map(lambda c: self.messenger.publish_response(
                'machine-requested',
                requested_checksum=c['checksum']
            )) \
            .flat_map(
                lambda cs:
                    self.messenger.get_command_listener()
                    .where(
                        lambda c:
                            c['confirmed_checksum'] == cs
                    )
                    .first()
                    .map(
                        lambda c:
                            self.messenger.publish_response(
                                'machine-confirmed',
                                requested_checksum=c['checksum'],
                                **(callback('', '', ''))
                            )
                    )
            )

    def listen_for_stats(self):
        pass
