from collections import namedtuple

EventQueues = namedtuple('EventQueues', (
    'state_machine',
    'dispatcher',
    'listener',
    'log_writer',
    'testing'
    ))
