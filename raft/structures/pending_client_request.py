from collections import namedtuple

PendingClientRequest = namedtuple(
    'PendingClientRequest',
    ('request_id', 'request_commit_index', 'request_receive_time')
)
