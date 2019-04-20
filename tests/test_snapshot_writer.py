import time
import multiprocessing as mp
import pathlib
import os
import pickle

import pytest

from raft.structures.messages import Snapshot
from raft.snapshot_writer import SnapshotWriter

def test_snapshot_gets_written_to_disk(snapshot_setup):
    with open('tmp/snapshot_5', 'rb') as f:
        contents = pickle.load(f)

    assert contents == Snapshot(commit_index=5, data={'x': 10})


@pytest.fixture(name='snapshot_setup')
def test_snapshot_gets_written_to_disk_setup():
    snapshot_location = 'tmp'
    state_machine_message_queue = mp.Queue()
    incoming_message_queue = mp.Queue()

    proc = mp.Process(
        target=start_snapshot_writer,
        args=(
            snapshot_location,
            state_machine_message_queue,
            incoming_message_queue
        )
    )

    proc.start()

    incoming_message_queue.put(Snapshot(commit_index=5, data={'x':10}))

    exists = wait_for_snapshot_file()
    if not exists:
        proc.kill()
        pytest.fail("Snapshot writer  process failed to create snapshot file")

    yield

    proc.kill()

def start_snapshot_writer(
    snapshot_location,
    state_machine_message_queue,
    incoming_message_queue
):
    snapshot_writer = SnapshotWriter(
        snapshot_location=snapshot_location,
        state_machine_message_queue=state_machine_message_queue,
        incoming_message_queue=incoming_message_queue
    )
    snapshot_writer.run()


def wait_for_snapshot_file():
    path = pathlib.Path('tmp/snapshot_5')
    start = time.time()
    # TODO Wrap in re-usable decorator since
    # we will use this pattern else where as well
    while time.time() - start < 5.0:
        if path.exists():
            return True
    return False




