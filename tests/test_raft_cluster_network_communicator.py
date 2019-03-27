import multiprocessing as mp
import time

import pytest

from raft.raft_cluster_network_communicator import RaftClusterNetworkCommunicator
from raft.raft_cluster_network_listener import RaftClusterNetworkListener

def test_sends_messages_to_connected_peers(peer_queues):
    received_messages = []
    for peer_queue in peer_queues:
        try:
            msg = peer_queue.get_nowait()
            if msg == b'Starting up':
                msg = peer_queue.get_nowait()
                received_messages.append(msg)
        except queue.Empty:
            pass
    assert len(received_messages) == 3
    expected = [
        {
            'message_type': 'AppendEntries',
            'args': {
                'term': 1,
                'leader_id': 123,
                'prev_log_index': 23,
                'prev_log_term': 0,
                'entries': ['set x 1'],
                'leader_commit': 24
            }
        },
        {
            'message_type': 'AppendEntries',
            'args': {
                'term': 1,
                'leader_id': 123,
                'prev_log_index': 23,
                'prev_log_term': 0,
                'entries': ['set x 1'],
                'leader_commit': 24
            }
        },
        {
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
    ]

    assert expected == received_messages

@pytest.fixture(name='peer_queues')
def start_peer_listeners():
    peers = [('localhost', 5000), ('localhost', 5001), ('localhost', 5003)]
    procs = []
    peer_queues = []
    for peer_id, address in enumerate(peers):
        q = mp.Queue()
        p = mp.Process(target=start_listener, args=(peer_id, address, q))
        peer_queues.append(q)
        procs.append(p)
        p.start()

    communicator = mp.Process(target=start_communicator, args=(peers,))
    communicator.start()

    time.sleep(1)
    yield peer_queues

    for p in procs:
        p.kill()

    communicator.kill()

def start_listener(peer_id, address, message_queue):
    listener = RaftClusterNetworkListener(address, peer_id, message_queue)
    listener.run()

def start_communicator(peers):
    q = mp.Queue()
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
    q.put(msg)
    communicator = RaftClusterNetworkCommunicator(peers,q)
    communicator.run()
