
from raft.structures.log_entry import LogEntry

_DEFAULT_LOG_INDEX = 0
_DEFAULT_LOG_TERM = 0
_DEFAULT_LOG_ENTRY = LogEntry(
    log_index=_DEFAULT_LOG_INDEX,
    term=_DEFAULT_LOG_TERM,
    command=None,
    data=None,
)

class MemoryLog:
    def __init__(self):
        self._log = []

    def append(self, entry):
        self._log.append(entry)

    def at(self, index):
        try:
            return self._log[index] if index < 0 else self._log[index - 1]
        except IndexError:
            return None

    def last_index(self):
        if self._log:
            return self._log[-1].log_index
        else:
            return _DEFAULT_LOG_INDEX

    def last_term(self):
        if self._log:
            return self._log[-1].term
        else:
            return _DEFAULT_LOG_TERM

    def last_entry(self):
        if self._log:
            return self._log[-1]
        else:
            return _DEFAULT_LOG_ENTRY

    def delete_from(self, index):
        del self._log[index - 1 :]

    def entries_from(self, index):
        return self._log[index - 1 :]

    def next_index(self):
        return len(self._log) + 1

    def __len__(self):
        return len(self._log)
