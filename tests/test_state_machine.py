import time
import multiprocessing as mp
import random
import pickle
import pathlib
import uuid

import pytest

from raft.state_machine import StateMachine
from raft.log_writer import LogWriter
from raft.snapshot_writer import SnapshotWriter
from raft.structures.node_config import NodeConfig
from raft.structures.log_entry import LogEntry
from raft.structures.messages import (
    AppendEntries,
    RequestVote,
    RequestVoteResponse,
    ClientRequest,
    AppendEntriesResponse,
    MajorityReplicated,
    SnapshotRequest,
    Snapshot,
    LocalStateSnapshotRequestForTesting,
)
from raft.structures.log_entry import LogEntry



def test_commands_get_logged_and_added_to(logged):
    # TODO Remove hardcoded log file name
    with open('tmp/0_1', 'rb') as f:
       contents = pickle.load(f)

    assert contents == [
       LogEntry(log_index=0, term=0, command='_set', data={'key': 'x', 'value': 1}),
       LogEntry(log_index=1, term=0, command='_set', data={'key': 'y', 'value': 2})
   ]



@pytest.fixture(name="logged")
def test_commands_get_logged():
    event_queues = {
       'state_machine': mp.Queue(),
       'communicator': mp.Queue(),
       'log_writer': mp.Queue(),
       'snapshot_writer': mp.Queue()

    }
    startup_state = "leader"

    state_machine_proc = mp.Process(
            target=start_state_machine,
            args=(
                event_queues,
                startup_state
            )
    )

    log_writer_proc = mp.Process(
        target=start_log_writer,
        args=('tmp', 2, event_queues['log_writer'])
    )

    state_machine_proc.start()
    log_writer_proc.start()

    message1 = ClientRequest(command='_set', data={'key': 'x', 'value': 1})
    message2 = ClientRequest(command='_set', data={'key': 'y', 'value': 2})
    event_queues['state_machine'].put(message1)
    event_queues['state_machine'].put(message2)

    exists = wait_for_log_file()
    if not exists:
        pytest.fail("LogWrite process failed to create logfile")

    yield

    state_machine_proc.kill()
    log_writer_proc.kill()
