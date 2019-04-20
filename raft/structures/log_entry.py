from collections import namedtuple

LogEntry = namedtuple('LogEntry', ['log_index', 'term', 'command', 'data'])
