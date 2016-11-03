# -*- coding: utf-8 -*-

"""
Copyright (c) 2016 Olof Montin <olof@montin.net>

This file is part of clique-connector.
"""

import os
from setuptools import setup


def read(filename):
    """Reads given filename and returns it as a string.
    """

    return open(os.path.join(os.path.dirname(__file__),
                             filename)).read()


setup(
    name='clique_connector',
    version='0.2',
    author='Olof Montin',
    author_email='olof@montin.net',
    description='Connector/messenger module for clique agents and apis.',
    keywords='clique messenger rabbitmq',
    url='https://bitbucket.org/bagis/clique-connector',
    packages=['clique_connector', 'test'],
    long_description=read('README.md'),
    install_requires=['amqpstorm',
                      'rx']
)
