# -*- coding: utf-8 -*-


def listener_error(stop, error):
    stop()
    raise error


def filter_message(test_values, message):
    body = message.json()
    return all(body[k] == test_values[k] for k in test_values)
