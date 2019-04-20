import time
import multiprocessing as mp
import pathlib
import os
import pickle

import pytest

from raft.structures.log_entry import LogEntry
from raft.log_writer import LogWriter

def test_buffered_log_entries_get_flushed_to_disk(setup):
    with open('tmp/0_1', 'rb') as f:
        log_contents = pickle.load(f)

    expected_log_contents = [
        LogEntry(log_index=0, term=0, command='_set', data='foo'),
        LogEntry(log_index=1, term=0, command='_set', data='bar')
    ]

    assert expected_log_contents == log_contents


@pytest.fixture(name='setup')
def test_buffered_log_entries_get_flushed_to_disk_setup():
    log_location = 'tmp'
    max_buffer_size = 2
    incoming_message_queue = mp.Queue()

    proc = mp.Process(
        target=start_log_writer,
        args=(log_location, max_buffer_size, incoming_message_queue)
    )
    proc.start()

    incoming_message_queue.put(
        LogEntry(log_index=0, term=0, command='_set', data='foo')
    )
    incoming_message_queue.put(
        LogEntry(log_index=1, term=0, command='_set', data='bar')
    )
    incoming_message_queue.put(
        LogEntry(log_index=2, term=0, command='_set', data='foo')
    )
    incoming_message_queue.put(
       LogEntry(log_index=3, term=0, command='_set', data='foobar')
    )

    exists = wait_for_log_file()
    if not exists:
        pytest.fail("LogWrite process failed to create logfile")

    yield

    proc.kill()

def start_log_writer(
    log_location,
    max_buffer_size,
    incoming_message_queue
):
    log_writer = LogWriter(
        log_location=log_location,
        max_buffer_size=max_buffer_size,
        incoming_message_queue=incoming_message_queue
    )
    log_writer.run()


def wait_for_log_file():
    path = pathlib.Path('tmp/0_1')
    start = time.time()
    # TODO Wrap in re-usable decorator since
    # we will use this pattern else where as well
    while time.time() - start < 5.0:
        if path.exists():
            return True
    return False




