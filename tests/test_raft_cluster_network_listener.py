import multiprocessing as mp
import queue
import socket
import struct
import json

import pytest

from raft.raft_cluster_network_listener import RaftClusterNetworkListener

def test_raft_cluster_network_listener(message_queue):
    """
    it reads, parses and enques for processing JSON messages received from peers
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 5000))
    s2.connect(('localhost', 5000))
    msg = {
        'message_type': 'AppendEntries',
        'args': {
            'term': 1,
            'leader_id': 123,
            'prev_log_index': 23,
            'prev_log_term': 0,
            'entries': ['set x 1'],
            'leader_commit': 24
        }
    }
    msg_bytes = json.dumps(msg).encode()
    payload = struct.pack('>I', len(msg_bytes)) + msg_bytes
    s.send(payload)
    s.send(payload)
    s2.send(payload)
    try:
        item = message_queue.get(timeout=5)
    except queue.Empty:
        s.close()
        pytest.fail('Raftlistener failed to communicate within allotted timeout of 5 seconds')
    s.close()

    assert item == msg


@pytest.fixture(name='message_queue')
def setup_raft_listener():
    q = mp.Queue()
    p = mp.Process(target=start_listener, args=(q,))
    p.start()
    try:
        q.get(timeout=3)
    except queue.Empty:
        p.kill()
        pytest.fail('Raftlistener failed to start within alloted timeout of 3 seconds ')

    yield q

    p.kill()

def start_listener(message_queue):
    listener = RaftClusterNetworkListener(('localhost', 5000), 1, message_queue)
    listener.run()
