from collections import namedtuple

EventQueues = namedtuple('EventQueues', (
    'state_machine',
    'dispatcher',
    'listener',
    'client_response',
    'log_writer',
    'testing'
    ))
