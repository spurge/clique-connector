# -*- coding: utf-8 -*-

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

    async def create_machine(self, name, image, cpu, mem):
        checksum = self.messenger.publish_command(
            command='machine-requested',
            name=name,
            image=image,
            cpu=cpu,
            mem=mem
        )

        return await self.messenger.get_response_listener() \
            .tap(print) \
            .where(lambda r: r['requested_checksum'] == checksum) \
            .first() \
            .tap(print) \
            .map(lambda r: self.messenger.publish_command(
                'machine-confirmed',
                confirmed_checksum=r['checksum']
            )) \
            .tap(lambda cs: print('map', cs)) \
            .flat_map(
                lambda cs:
                    self.messenger.get_response_listener()
                    .tap(lambda r: print('flatmap', cs, r))
                    .where(
                        lambda r:
                            r['requested_checksum'] == cs
                    )
                    .first()
                    .tap(print)
            ) \
            .map(lambda r: {key: value for key, value in r if key in [
                'host', 'username', 'password'
            ]}) \
            .tap(print)

    def stop_machine(self, name):
        pass

    def send_stats(self):
        pass

    async def wait_for_machine(self, callback):
        return await self.messenger.get_command_listener() \
            .where(lambda c: c['command'] == 'machine-requested') \
            .tap(print) \
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
                    .tap(print)
                    .map(
                        lambda c:
                            self.messenger.publish_response(
                                'machine-confirmed',
                                requested_checksum=c['checksum'],
                                **(callback('', '', ''))
                            )
                    )
            ) \
            .tap(print)

    def listen_for_stats(self):
        pass
